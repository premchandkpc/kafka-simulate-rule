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
	"github.com/flowrule/flowrule/internal/rules"
	"github.com/flowrule/flowrule/internal/runtime/scheduler"
	"github.com/flowrule/flowrule/internal/runtime/shard"
	"github.com/flowrule/flowrule/internal/services/batches"
	svceffects "github.com/flowrule/flowrule/internal/services/effects"
	svcevents "github.com/flowrule/flowrule/internal/services/events"
	"github.com/flowrule/flowrule/internal/storage"
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
	db, err := storage.Open(ctx, cfg.Database)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer db.Close()

	if err := db.Initialize(ctx); err != nil {
		log.Fatalf("migrations: %v", err)
	}

	repositories := db.Repositories()

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
	// Create services
	workerID := cfg.WorkerID()

	eventsSvc := svcevents.NewService(
		compiler, evaluator, repositories.Quarantine, clock,
		repositories.Transactions, repositories.NewTxRepositories,
	)
	effectsSvc := svceffects.NewService(repositories.Outbox, effectSender, repositories.Quarantine, clock, workerID)

	// Batch service
	batchMode := domain.BatchMode(cfg.Worker.BatchMode)
	batchCfg := domain.BatchConfig{
		Mode:     batchMode,
		MaxBatch: cfg.Worker.BatchMax,
		Window:   cfg.BatchWindow(),
	}
	batchSvc := batches.NewService(repositories.Batches, eventsSvc, clock, batchCfg)

	// Create lease manager
	leaseManager := shard.NewLeaseManager(repositories.ShardLeases, clock, workerID, cfg.Worker.NumShards, cfg.LeaseTTL())

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
	sched := scheduler.NewScheduler(repositories.ScheduledEvents, consumer, clock, cfg.SchedulerPollInterval(), cfg.Worker.SchedulerBatchSize)
	sched.Start(ctx)
	defer sched.Stop()

	// Run worker
	worker.Run(ctx)
}
