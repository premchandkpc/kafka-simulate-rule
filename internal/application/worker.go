package application

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/flowrule/flowrule/internal/runtime/keyqueue"
	"github.com/flowrule/flowrule/internal/runtime/shard"
)

// WorkerConfig holds configuration for the worker.
type WorkerConfig struct {
	MaxGlobalInFlight      int
	FetchBatchSize         int
	EffectPublishInterval  time.Duration
	EffectPublishBatchSize int
	RetryDelay             time.Duration
	MaxPerKeyQueue         int
	KeyQueueWorkers        int
	HotKeyThreshold        int
	HotKeyCheckInterval    time.Duration
	HotKeyCallback         func(partitionKey string, depth int)
	RequireSharded         bool // If true, worker requires a sharded consumer and lease manager
}

// Worker owns the runtime loop for a broker-backed rules worker.
// It separates transport bootstrap from business orchestration so the main
// entrypoint stays thin and the runtime behavior remains testable.
type Worker struct {
	events          ports.EventProcessor
	effects         ports.EffectPublisher
	batches         ports.BatchProcessor
	batchInterval   time.Duration
	clock           ports.Clock
	leaseManager    *shard.LeaseManager
	keyQueue        *keyqueue.KeyQueue
	workerID        string
	numShards       uint32
	config          WorkerConfig
	shardedConsumer ports.ShardedConsumer
}

// DefaultWorkerConfig returns sensible defaults.
func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		MaxGlobalInFlight:      100,
		FetchBatchSize:         100,
		EffectPublishInterval:  5 * time.Second,
		EffectPublishBatchSize: 10,
		RetryDelay:             5 * time.Second,
		MaxPerKeyQueue:         100,
		KeyQueueWorkers:        1,
		HotKeyThreshold:        1000,
		HotKeyCheckInterval:    10 * time.Second,
		RequireSharded:         true, // Production workers require sharded mode
	}
}

func NewWorker(
	consumer ports.ShardedConsumer,
	events ports.EventProcessor,
	effects ports.EffectPublisher,
	batches ports.BatchProcessor,
	batchInterval time.Duration,
	clock ports.Clock,
	leaseManager *shard.LeaseManager,
	workerID string,
	numShards uint32,
	config WorkerConfig,
) (*Worker, error) {
	if consumer == nil {
		return nil, errors.New("worker consumer is required")
	}
	if events == nil {
		return nil, errors.New("worker event processor is required")
	}
	if effects == nil {
		return nil, errors.New("worker effect publisher is required")
	}
	if clock == nil {
		return nil, errors.New("worker clock is required")
	}
	if workerID == "" {
		return nil, errors.New("worker ID is required")
	}
	if numShards == 0 {
		numShards = 4096
	}

	defaults := DefaultWorkerConfig()
	// Preserve the legacy zero-value behavior: an omitted WorkerConfig means
	// production-safe defaults, including mandatory sharded mode.
	if config.MaxGlobalInFlight == 0 {
		config = defaults
	}
	if config.MaxGlobalInFlight < 0 {
		config.MaxGlobalInFlight = defaults.MaxGlobalInFlight
	}
	if config.FetchBatchSize <= 0 {
		config.FetchBatchSize = defaults.FetchBatchSize
	}
	if config.EffectPublishInterval <= 0 {
		config.EffectPublishInterval = defaults.EffectPublishInterval
	}
	if config.EffectPublishBatchSize <= 0 {
		config.EffectPublishBatchSize = defaults.EffectPublishBatchSize
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = defaults.RetryDelay
	}
	if config.MaxPerKeyQueue <= 0 {
		config.MaxPerKeyQueue = defaults.MaxPerKeyQueue
	}
	if config.KeyQueueWorkers <= 0 {
		config.KeyQueueWorkers = defaults.KeyQueueWorkers
	}
	if config.HotKeyThreshold <= 0 {
		config.HotKeyThreshold = defaults.HotKeyThreshold
	}
	if config.HotKeyCheckInterval <= 0 {
		config.HotKeyCheckInterval = defaults.HotKeyCheckInterval
	}
	if config.FetchBatchSize > config.MaxGlobalInFlight {
		return nil, fmt.Errorf("worker fetch batch size (%d) must not exceed max global in-flight (%d)", config.FetchBatchSize, config.MaxGlobalInFlight)
	}

	if !config.RequireSharded {
		return nil, errors.New("non-sharded worker mode is unsupported because event processing requires shard fencing")
	}
	if leaseManager == nil {
		return nil, errors.New("lease manager is required for fenced event processing")
	}

	return &Worker{
		events:          events,
		effects:         effects,
		batches:         batches,
		batchInterval:   batchInterval,
		clock:           clock,
		leaseManager:    leaseManager,
		workerID:        workerID,
		numShards:       numShards,
		config:          config,
		shardedConsumer: consumer,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("worker context is required")
	}
	if w.leaseManager != nil {
		if err := w.leaseManager.Start(ctx); err != nil {
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			w.leaseManager.Stop(shutdownCtx)
			cancel()
			return fmt.Errorf("start lease manager: %w", err)
		}
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			w.leaseManager.Stop(shutdownCtx)
		}()
	}

	w.keyQueue = keyqueue.NewKeyQueue(
		w.config.MaxGlobalInFlight,
		w.config.MaxPerKeyQueue,
		w.config.KeyQueueWorkers,
		func(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error {
			return w.processDelivery(ctx, env, delivery, fencingToken, vshard, workerID)
		},
		w.clock.Now,
		w.config.HotKeyThreshold,
		w.config.HotKeyCheckInterval,
		w.config.HotKeyCallback,
	)
	w.keyQueue.Start(ctx)
	defer w.keyQueue.Stop()

	var background sync.WaitGroup
	background.Add(1)
	go func() {
		defer background.Done()
		w.publishLoop(ctx)
	}()
	if w.batches != nil && w.batchInterval > 0 {
		background.Add(1)
		go func() {
			defer background.Done()
			w.batchLoop(ctx)
		}()
	}
	// Wait for periodic loops before stopping the queue, releasing leases,
	// and allowing the caller to close shared resources.
	defer background.Wait()

	w.runSharded(ctx)
	return nil
}
