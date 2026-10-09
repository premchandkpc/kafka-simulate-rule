package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/flowrule/flowrule/internal/adapters/effects"
	"github.com/flowrule/flowrule/internal/adapters/nats"
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
	defer func() { _ = obs.Shutdown(ctx) }()

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
	natsCfg := nats.Config{
		NatsURL:    cfg.NATS.URL,
		Stream:     cfg.NATS.Stream,
		Consumer:   cfg.NATS.Consumer,
		Subjects:   []string{"events"},
		AckWait:    30 * time.Second,
		MaxDeliver: 10,
		NumShards:  cfg.Worker.NumShards,
	}
	consumer, err := nats.NewConsumer(ctx, natsCfg)
	if err != nil {
		log.Fatalf("NATS JetStream: %v", err)
	}
	defer consumer.Close()

	// Create core services
	compiler := rules.NewCompiler(rules.DefaultLimits())
	evaluator := rules.NewEvaluator()
	clock := domain.SystemClock{}

	// Create effect sender - use HTTP destination in production, fail if not configured
	var effectSender ports.EffectSender
	if cfg.Worker.EffectDestinationURL != "" {
		effectSender = effects.NewHTTPDestination(cfg.Worker.EffectDestinationURL)
	} else if cfg.App.Env == "development" || cfg.App.Env == "test" {
		effectSender = effects.NewFakeDestination()
	} else {
		log.Fatalf("EFFECT_DESTINATION_URL is required in production environment")
	}

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
	sched := scheduler.NewScheduler(repositories.ScheduledEvents, consumer, clock, cfg.SchedulerPollInterval(), cfg.Worker.SchedulerBatchSize, cfg.Worker.NumShards)
	sched.Start(ctx)
	defer sched.Stop()

	// Run worker
	worker.Run(ctx)
}
