package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

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

const (
	serviceName  = "flowrule-worker"
	eventSubject = "events"
)

// main owns process exit behavior. run returns errors so deferred cleanup executes.
func main() {
	if err := run(); err != nil {
		log.Printf("%s stopped with error: %v", serviceName, err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	obsConfig := observability.DefaultConfig()
	obsConfig.ServiceName = serviceName
	obs, err := observability.New(obsConfig)
	if err != nil {
		return fmt.Errorf("initialize observability: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeoutDuration())
		defer cancel()
		if err := obs.Shutdown(shutdownCtx); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("shutdown observability: %w", err))
		}
	}()

	obs.Logger.Info("worker starting", "environment", cfg.App.Env)

	if metricsAddr := strings.TrimSpace(os.Getenv("FLOWRULE_METRICS_ADDR")); metricsAddr != "" {
		shutdownMetrics, err := startMetricsServer(metricsAddr, os.Getenv("FLOWRULE_CONSOLE_ORIGIN"))
		if err != nil {
			return err
		}
		defer func() {
			if err := shutdownMetrics(); err != nil {
				runErr = errors.Join(runErr, err)
			}
		}()
		obs.Logger.Info("worker metrics endpoint enabled", "address", metricsAddr)
	}

	db, err := storage.Open(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	// Storage.Close currently has no error return; keep it deferred so all
	// startup failure paths release the database resources.
	defer db.Close()

	if err := db.Initialize(ctx); err != nil {
		return fmt.Errorf("initialize storage: %w", err)
	}
	repositories := db.Repositories()

	natsCfg := nats.Config{
		NatsURL:    cfg.NATS.URL,
		Stream:     cfg.NATS.Stream,
		Consumer:   cfg.NATS.Consumer,
		Subjects:   []string{eventSubject},
		AckWait:    cfg.NATSAckWait(),
		MaxDeliver: cfg.Worker.NATSMaxDeliver,
		NumShards:  cfg.Worker.NumShards,
	}
	consumer, err := nats.NewConsumer(ctx, natsCfg)
	if err != nil {
		return fmt.Errorf("initialize NATS JetStream: %w", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close NATS consumer: %w", err))
		}
	}()

	compiler := rules.NewCompiler(rules.DefaultLimits())
	clock := domain.SystemClock{}
	evaluator := rules.NewEvaluator(clock)

	var externalSender ports.EffectSender
	switch {
	case cfg.Worker.EffectDestinationURL != "":
		externalSender = effects.NewHTTPDestination(cfg.Worker.EffectDestinationURL)
	case cfg.App.Env == "development" || cfg.App.Env == "test":
		externalSender = effects.NewFakeDestination()
	default:
		return fmt.Errorf("EFFECT_DESTINATION_URL is required when APP_ENV=%q", cfg.App.Env)
	}

	// Route emitted child events back into the same subject family consumed by
	// this worker. Only command effects leave the broker for an external sender.
	serviceSender := effects.NewDomainServiceSender(
		repositories.DomainServices,
		cfg.App.Env == "development" || cfg.App.Env == "dev" || cfg.App.Env == "test",
	)
	effectSender := svceffects.NewRouterWithConfig(
		consumer, externalSender, eventSubject, cfg.Worker.NumShards, serviceSender,
	)
	workerID := cfg.WorkerID()
	eventsSvc := svcevents.NewService(
		compiler,
		evaluator,
		repositories.Quarantine,
		clock,
		repositories.Transactions,
		repositories.NewEventTxRepositories,
	)
	effectsSvc := svceffects.NewService(
		repositories.Outbox,
		effectSender,
		repositories.Quarantine,
		clock,
		workerID,
	)

	batchCfg := domain.BatchConfig{
		Mode:     domain.BatchMode(cfg.Worker.BatchMode),
		MaxBatch: cfg.Worker.BatchMax,
		Window:   cfg.BatchWindow(),
	}
	batchSvc := batches.NewService(repositories.Batches, eventsSvc, clock, batchCfg)

	leaseManager := shard.NewLeaseManagerWithReplicaCountAndRenewInterval(
		repositories.ShardLeases,
		clock,
		workerID,
		cfg.Worker.NumShards,
		cfg.Worker.WorkerReplicas,
		cfg.LeaseTTL(),
		cfg.LeaseRenewalInterval(),
	)
	workerConfig := application.DefaultWorkerConfig()
	workerConfig.MaxGlobalInFlight = cfg.Worker.MaxGlobalInFlight
	workerConfig.FetchBatchSize = cfg.Worker.FetchBatchSize
	workerConfig.EffectPublishInterval = cfg.EffectPublishInterval()
	workerConfig.EffectPublishBatchSize = cfg.Worker.EffectPublishBatchSize
	workerConfig.RetryDelay = cfg.EventRetryDelay()
	workerConfig.MaxPerKeyQueue = cfg.Worker.MaxPerKeyQueue
	workerConfig.KeyQueueWorkers = cfg.Worker.KeyQueueWorkers
	workerConfig.HotKeyThreshold = cfg.Worker.HotKeyThreshold
	workerConfig.HotKeyCheckInterval = cfg.HotKeyCheckInterval()

	worker, err := application.NewWorker(
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
	if err != nil {
		return fmt.Errorf("construct worker: %w", err)
	}
	sched := scheduler.NewScheduler(
		repositories.ScheduledEvents,
		consumer,
		clock,
		cfg.SchedulerPollInterval(),
		cfg.Worker.SchedulerBatchSize,
		cfg.Worker.NumShards,
	)

	obs.Logger.Info("worker initialized", "worker_id", workerID, "shards", cfg.Worker.NumShards)
	sched.Start(ctx)
	defer sched.Stop()

	if err := worker.Run(ctx); err != nil {
		return fmt.Errorf("run worker: %w", err)
	}
	obs.Logger.Info("worker stopped", "worker_id", workerID)
	return nil
}
