package main

import (
	"context"
	"log"
		"os/signal"
	"syscall"
	"time"

	"github.com/flowrule/flowrule/internal/adapters/effects"
	"github.com/flowrule/flowrule/internal/adapters/sql"
	"github.com/flowrule/flowrule/internal/application"
	"github.com/flowrule/flowrule/internal/config"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/observability"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/flowrule/flowrule/internal/rules"
	"github.com/flowrule/flowrule/internal/runtime/scheduler"
	"github.com/flowrule/flowrule/internal/runtime/shard"
	"github.com/flowrule/flowrule/internal/services/batches"
	svceffects "github.com/flowrule/flowrule/internal/services/effects"
	svcevents "github.com/flowrule/flowrule/internal/services/events"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Initialize observability
	obsConfig := observability.DefaultConfig()
	obsConfig.ServiceName = "flowrule-worker"
	obs, err := observability.New(obsConfig)
	if err != nil {
		log.Fatalf("observability init: %v", err)
	}
	defer obs.Shutdown(ctx)

	obs.Logger.Info("worker starting")

	// Initialize database
	db, err := sql.New(ctx, cfg.Database.DSN, cfg.Database.MigrationsDir)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer db.Close()

	if err := db.RunMigrations(ctx); err != nil {
		log.Fatalf("migrations: %v", err)
	}

	pool := db.Pool()

	// Initialize NATS
	consumer, err := sql.NewJetStreamConsumer(ctx, cfg.NATS.URL, cfg.NATS.Stream, cfg.Worker.NumShards)
	if err != nil {
		log.Fatalf("NATS JetStream: %v", err)
	}
	defer consumer.Close()

	// Create core services
	compiler := rules.NewCompiler(rules.DefaultLimits())
	evaluator := rules.NewEvaluator()
	clock := domain.SystemClock{}
	effectSender := effects.NewFakeDestination()

	// Create repositories
	quarantineRepo := sql.NewQuarantineRepository(pool)
	shardLeaseRepo := sql.NewShardLeaseRepository(pool)

	// Create transaction manager
	txManager := sql.NewTransactionManager(pool)

	// Create repository factory
	newRepos := func(tx ports.Transaction) ports.TxRepos {
		txQuerier := sql.NewTxQuerier(tx.(*sql.Tx))
		return ports.TxRepos{
			Inbox:           sql.NewInboxRepository(txQuerier),
			Activations:     sql.NewActivationRepository(txQuerier),
			RuleRepo:        sql.NewRuleRepository(txQuerier),
			Executions:      sql.NewExecutionRepository(txQuerier),
			Outbox:          sql.NewOutboxRepository(txQuerier),
			ShardLeases:     sql.NewShardLeaseRepository(txQuerier),
			Workflow:        sql.NewWorkflowRepository(txQuerier),
			ScheduledEvents: sql.NewScheduledEventRepository(txQuerier),
			Batches:         sql.NewBatchRepository(txQuerier),
			Quarantine:      sql.NewQuarantineRepository(txQuerier),
		}
	}

// Create services
	workerID := cfg.WorkerID()

	eventsSvc := svcevents.NewService(compiler, evaluator, quarantineRepo, clock, txManager, newRepos)
	outboxRepo := sql.NewOutboxRepository(pool)
	effectsSvc := svceffects.NewService(outboxRepo, effectSender, quarantineRepo, clock, workerID)

	// Batch service
	batchMode := domain.BatchMode(cfg.Worker.BatchMode)
	batchCfg := domain.BatchConfig{
		Mode:     batchMode,
		MaxBatch: cfg.Worker.BatchMax,
		Window:   cfg.BatchWindow(),
	}
	batchRepo := sql.NewBatchRepository(pool)
	batchSvc := batches.NewService(batchRepo, eventsSvc, clock, batchCfg)

	// Create lease manager
	leaseManager := shard.NewLeaseManager(shardLeaseRepo, clock, workerID, cfg.Worker.NumShards, cfg.LeaseTTL())

	workerConfig := application.WorkerConfig{
		MaxGlobalInFlight:   cfg.Worker.MaxGlobalInFlight,
		MaxPerKeyQueue:      cfg.Worker.MaxPerKeyQueue,
		KeyQueueWorkers:     cfg.Worker.KeyQueueWorkers,
		HotKeyThreshold:     1000,
		HotKeyCheckInterval: 10 * time.Second,
	}

	worker := application.NewWorker(
		consumer,
		eventsSvc,
		effectsSvc,
		batchSvc,
		cfg.SchedulerPollInterval(),
		clock,
		leaseManager,
		workerID,
		cfg.Worker.NumShards,
		workerConfig,
	)

	obs.Logger.Info("worker started",
		"worker_id", workerID,
		"shards", cfg.Worker.NumShards,
	)

	// Start scheduler
	schedRepo := sql.NewScheduledEventRepository(pool)
	sched := scheduler.NewScheduler(schedRepo, consumer, clock, cfg.SchedulerPollInterval(), cfg.Worker.SchedulerBatchSize)
	sched.Start(ctx)
	defer sched.Stop()

	// Run worker
	worker.Run(ctx)
}