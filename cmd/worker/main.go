package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/flowrule/flowrule/internal/adapters/effects"
	"github.com/flowrule/flowrule/internal/adapters/nats"
	"github.com/flowrule/flowrule/internal/adapters/sql"
	"github.com/flowrule/flowrule/internal/application"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/flowrule/flowrule/internal/rules"
	"github.com/flowrule/flowrule/internal/runtime/scheduler"
	"github.com/flowrule/flowrule/internal/runtime/shard"
	"github.com/flowrule/flowrule/internal/services/batches"
	svceffects "github.com/flowrule/flowrule/internal/services/effects"
	svcevents "github.com/flowrule/flowrule/internal/services/events"
)

func envDuration(name string, defaultMs int) time.Duration {
	if v := os.Getenv(name); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return time.Duration(defaultMs) * time.Millisecond
}

func envInt(name string, defaultVal int) int {
	if v := os.Getenv(name); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}

func envBatchMode(name string) domain.BatchMode {
	if v := os.Getenv(name); v != "" {
		return domain.BatchMode(v)
	}
	return domain.BatchModeNone
}

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/flowrule?sslmode=disable"
	}
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	migrationsDir := os.Getenv("MIGRATIONS_DIR")
	if migrationsDir == "" {
		migrationsDir = "migrations"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	db, err := sql.New(ctx, dsn, migrationsDir)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer db.Close()

	if err := db.RunMigrations(ctx); err != nil {
		log.Fatalf("migrations: %v", err)
	}

	numShards := uint32(envInt("NUM_SHARDS", 4096))

	consumer, err := nats.NewConsumer(ctx, nats.Config{
		NatsURL:    natsURL,
		Stream:     "flowrule",
		Consumer:   "flowrule-worker",
		Subjects:   []string{"events"},
		AckWait:    30 * time.Second,
		MaxDeliver: 10,
		NumShards:  numShards,
	})
	if err != nil {
		log.Fatalf("nats consumer: %v", err)
	}
	defer consumer.Close()
	publisher, err := nats.NewPublisher(nats.Config{NatsURL: natsURL, Stream: "flowrule", NumShards: numShards})
	if err != nil {
		log.Fatalf("nats publisher: %v", err)
	}
	defer publisher.Close()

	compiler := rules.NewCompiler(rules.DefaultLimits())
	evaluator := rules.NewEvaluator()
	clock := domain.SystemClock{}
	effectSender := effects.NewFakeDestination()
	pool := db.Pool()
	quarantineRepo := sql.NewQuarantineRepository(pool)

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		hostname, _ := os.Hostname()
		workerID = hostname + "-" + strconv.Itoa(os.Getpid())
	}
	leaseTTL := envDuration("LEASE_TTL_MS", 30000)

	shardLeaseRepo := sql.NewShardLeaseRepository(pool)
	leaseManager := shard.NewLeaseManager(shardLeaseRepo, clock, workerID, numShards, leaseTTL)

	beginTx := func(ctx context.Context) (ports.Tx, error) {
		pgxTx, err := pool.Begin(ctx)
		if err != nil {
			return nil, err
		}
		return sql.NewTx(pgxTx), nil
	}
	newRepos := func(db interface{}) svcevents.TxRepos {
		q := db.(sql.Querier)
		return svcevents.TxRepos{
			Inbox:       sql.NewInboxRepository(q),
			Activations: sql.NewActivationRepository(q),
			RuleRepo:    sql.NewRuleRepository(q),
			Executions:  sql.NewExecutionRepository(q),
			Outbox:      sql.NewOutboxRepository(q),
			ShardLeases: sql.NewShardLeaseRepository(q),
		}
	}

	eventsSvc := svcevents.NewService(compiler, evaluator, quarantineRepo, clock, beginTx, newRepos)
	outboxRepo := sql.NewOutboxRepository(pool)
	effectsSvc := svceffects.NewService(outboxRepo, effectSender, quarantineRepo, clock)

	batchMode := envBatchMode("BATCH_MODE")
	batchCfg := domain.BatchConfig{
		Mode:     batchMode,
		MaxBatch: envInt("BATCH_MAX", 100),
		Window:   envDuration("BATCH_WINDOW_MS", 30000),
	}
	batchRepo := sql.NewBatchRepository(pool)
	batchSvc := batches.NewService(batchRepo, eventsSvc, clock, batchCfg)
	batchInterval := envDuration("BATCH_INTERVAL_MS", 5000)

	workerConfig := application.WorkerConfig{
		MaxGlobalInFlight: envInt("MAX_GLOBAL_IN_FLIGHT", 100),
		MaxPerKeyQueue:    envInt("MAX_PER_KEY_QUEUE", 100),
		KeyQueueWorkers:   envInt("KEY_QUEUE_WORKERS", 1),
	}
	worker := application.NewWorker(consumer, eventsSvc, effectsSvc, batchSvc, batchInterval, clock, leaseManager, workerID, numShards, workerConfig)
	log.Printf("worker started (id=%s, shards=%d), fetching events... (batch mode: %s, max: %d, window: %v, interval: %v)", workerID, numShards, batchMode, batchCfg.MaxBatch, batchCfg.Window, batchInterval)

	// Start scheduler
	schedRepo := sql.NewScheduledEventRepository(pool)
	sched := scheduler.NewScheduler(schedRepo, publisher, clock, envDuration("SCHEDULER_POLL_MS", 5000), envInt("SCHEDULER_BATCH_SIZE", 100))
	sched.Start(ctx)
	defer sched.Stop()

	worker.Run(ctx)
}
