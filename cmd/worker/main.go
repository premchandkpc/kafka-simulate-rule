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
	"github.com/flowrule/flowrule/internal/adapters/mongo"
	"github.com/flowrule/flowrule/internal/application"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/factory"
	"github.com/flowrule/flowrule/internal/observability"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/flowrule/flowrule/internal/rules"
	"github.com/flowrule/flowrule/internal/runtime/scheduler"
	"github.com/flowrule/flowrule/internal/services/batches"
	svceffects "github.com/flowrule/flowrule/internal/services/effects"
	svcevents "github.com/flowrule/flowrule/internal/services/events"
)

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

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

	// Create factory with config
	fac := factory.NewFactory(factory.Config{
		MongoDB: factory.MongoConfig{
			URI:         getEnv("MONGODB_URI", "mongodb://localhost:27017"),
			Database:    getEnv("MONGODB_DATABASE", "flowrule"),
			MaxPool:     uint64(envInt("MONGODB_MAX_POOL", 20)),
			MinPool:     uint64(envInt("MONGODB_MIN_POOL", 2)),
			MaxConnIdle: envDuration("MONGODB_MAX_CONN_IDLE_MS", 300000),
		},
		NATS: factory.NATSConfig{
			URL:        getEnv("NATS_URL", "nats://localhost:4222"),
			Stream:     "flowrule",
			Consumer:   "flowrule-worker",
			Subjects:   []string{"events"},
			AckWait:    envDuration("NATS_ACK_WAIT_MS", 30000),
			MaxDeliver: envInt("NATS_MAX_DELIVER", 10),
			NumShards:  uint32(envInt("NUM_SHARDS", 4096)),
		},
		Redis: factory.RedisConfig{
			Addr:         getEnv("REDIS_ADDR", "localhost:6379"),
			Password:     getEnv("REDIS_PASSWORD", ""),
			DB:           envInt("REDIS_DB", 0),
			PoolSize:     envInt("REDIS_POOL_SIZE", 10),
			MinIdleConns: envInt("REDIS_MIN_IDLE", 2),
			MaxRetries:   3,
			DialTimeout:  envDuration("REDIS_DIAL_TIMEOUT_MS", 5000),
			ReadTimeout:  envDuration("REDIS_READ_TIMEOUT_MS", 3000),
			WriteTimeout: envDuration("REDIS_WRITE_TIMEOUT_MS", 3000),
		},
		Worker: factory.WorkerConfig{
			MaxGlobalInFlight: envInt("MAX_GLOBAL_IN_FLIGHT", 100),
			MaxPerKeyQueue:    envInt("MAX_PER_KEY_QUEUE", 100),
			KeyQueueWorkers:   envInt("KEY_QUEUE_WORKERS", 1),
		},
		Scheduler: factory.SchedulerConfig{
			PollInterval: envDuration("SCHEDULER_POLL_MS", 5000),
			BatchSize:    envInt("SCHEDULER_BATCH_SIZE", 100),
		},
		KeyQueue: factory.KeyQueueConfig{
			MaxGlobalInFlight:   envInt("KEYQUEUE_MAX_GLOBAL_IN_FLIGHT", 100),
			MaxPerKeyQueue:      envInt("KEYQUEUE_MAX_PER_KEY", 100),
			WorkerCount:         envInt("KEYQUEUE_WORKERS", 1),
			HotKeyThreshold:     envInt("KEYQUEUE_HOT_KEY_THRESHOLD", 1000),
			HotKeyCheckInterval: envDuration("KEYQUEUE_HOT_KEY_CHECK_MS", 10000),
		},
		ShardLease: factory.ShardLeaseConfig{
			NumShards:       uint32(envInt("NUM_SHARDS", 4096)),
			LeaseTTL:        envDuration("LEASE_TTL_MS", 30000),
			RenewalInterval: envDuration("LEASE_RENEWAL_INTERVAL_MS", 10000),
		},
		Batch: factory.BatchConfig{
			Mode:     envBatchMode("BATCH_MODE"),
			MaxBatch: envInt("BATCH_MAX", 100),
			Window:   envDuration("BATCH_WINDOW_MS", 30000),
		},
	})

	// Initialize MongoDB
	mongoDB, err := fac.MongoDB(ctx)
	if err != nil {
		obs.Logger.Error("mongodb connection failed", "error", err)
		return
	}
	defer mongoDB.Close()

	// Initialize Redis
	redisClient, err := fac.RedisClient(ctx)
	if err != nil {
		obs.Logger.Error("redis connection failed", "error", err)
		return
	}
	defer redisClient.Close()

	// Initialize NATS
	consumer, err := fac.NATSConsumer(ctx)
	if err != nil {
		obs.Logger.Error("nats consumer failed", "error", err)
		return
	}
	defer consumer.Close()

	publisher, err := fac.NATSPublisher(ctx)
	if err != nil {
		obs.Logger.Error("nats publisher failed", "error", err)
		return
	}
	defer publisher.Close()

	// Create transaction manager
	txManager := fac.TransactionManager(mongoDB)

	// Create core services
	compiler := rules.NewCompiler(rules.DefaultLimits())
	evaluator := rules.NewEvaluator()
	clock := domain.SystemClock{}
	effectSender := effects.NewFakeDestination()

	// Create repositories from database (non-transactional)
	database := mongoDB.Database()
	quarantineRepo := mongo.NewQuarantineRepository(mongo.NewQuerier(database))
	// shardLeaseRepo := mongo.NewShardLeaseRepository(mongo.NewQuerier(database))

	// Create lease manager
	workerID := getEnv("WORKER_ID", "")
	if workerID == "" {
		hostname, _ := os.Hostname()
		workerID = hostname + "-" + strconv.Itoa(os.Getpid())
	}

	leaseManager := fac.ShardManager(mongoDB, clock, workerID)

	// Create repository factory
	newRepos := func(tx ports.Transaction) ports.TxRepos {
		txQuerier := mongo.NewTxQuerier(tx.(*mongo.Transaction).Session())
		return ports.TxRepos{
			Inbox:           mongo.NewInboxRepository(txQuerier),
			Activations:     mongo.NewActivationRepository(txQuerier),
			RuleRepo:        mongo.NewRepository(txQuerier),
			Executions:      mongo.NewExecutionRepository(txQuerier),
			Outbox:          mongo.NewOutboxRepository(txQuerier),
			ShardLeases:     mongo.NewShardLeaseRepository(txQuerier),
			Workflow:        mongo.NewWorkflowRepository(txQuerier),
			ScheduledEvents: mongo.NewScheduledEventRepository(txQuerier),
			Batches:         mongo.NewBatchRepository(txQuerier),
			Quarantine:      mongo.NewQuarantineRepository(txQuerier),
		}
	}

	// Create services
	eventsSvc := svcevents.NewService(compiler, evaluator, quarantineRepo, clock, txManager, newRepos)
	outboxRepo := mongo.NewOutboxRepository(mongo.NewQuerier(database))
	effectsSvc := svceffects.NewService(outboxRepo, effectSender, quarantineRepo, clock)

	// Batch service
	batchMode := envBatchMode("BATCH_MODE")
	batchCfg := domain.BatchConfig{
		Mode:     batchMode,
		MaxBatch: envInt("BATCH_MAX", 100),
		Window:   envDuration("BATCH_WINDOW_MS", 30000),
	}
	batchRepo := mongo.NewBatchRepository(mongo.NewQuerier(database))
	batchSvc := batches.NewService(batchRepo, eventsSvc, clock, batchCfg)
	batchInterval := envDuration("BATCH_INTERVAL_MS", 5000)

	workerConfig := application.WorkerConfig{
		MaxGlobalInFlight:   envInt("MAX_GLOBAL_IN_FLIGHT", 100),
		MaxPerKeyQueue:      envInt("MAX_PER_KEY_QUEUE", 100),
		KeyQueueWorkers:     envInt("KEY_QUEUE_WORKERS", 1),
		HotKeyThreshold:     envInt("KEYQUEUE_HOT_KEY_THRESHOLD", 1000),
		HotKeyCheckInterval: envDuration("KEYQUEUE_HOT_KEY_CHECK_MS", 10000),
	}

	worker := application.NewWorker(
		consumer,
		eventsSvc,
		effectsSvc,
		batchSvc,
		batchInterval,
		clock,
		leaseManager,
		workerID,
		uint32(envInt("NUM_SHARDS", 4096)),
		workerConfig,
	)

	obs.Logger.Info("worker started",
		"worker_id", workerID,
		"shards", envInt("NUM_SHARDS", 4096),
	)

	// Start scheduler
	schedRepo := mongo.NewScheduledEventRepository(mongo.NewQuerier(database))
	sched := scheduler.NewScheduler(schedRepo, publisher, clock, envDuration("SCHEDULER_POLL_MS", 5000), envInt("SCHEDULER_BATCH_SIZE", 100))
	sched.Start(ctx)
	defer sched.Stop()

	// Run worker
	worker.Run(ctx)
}
