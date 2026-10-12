package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/flowrule/flowrule/internal/ports"
	"github.com/flowrule/flowrule/internal/runtime/keyqueue"
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

// shardedConsumer is an internal interface for consumers that support shard management
type shardedConsumer interface {
	EnsureShardConsumer(ctx context.Context, shard uint32) error
	RemoveShardConsumer(ctx context.Context, shard uint32) error
	FetchShards(ctx context.Context, maxMessages int, shards []uint32) ([]ports.Delivery, error)
	OwnedShards() []uint32
}

// Worker owns the runtime loop for a broker-backed rules worker.
// It separates transport bootstrap from business orchestration so the main
// entrypoint stays thin and the runtime behavior remains testable.
type Worker struct {
	consumer        ports.BrokerConsumer
	events          ports.EventProcessor
	effects         ports.EffectPublisher
	batches         ports.BatchProcessor
	batchInterval   time.Duration
	clock           ports.Clock
	shardService    ports.ShardService
	keyQueue        *keyqueue.KeyQueue
	workerID        string
	numShards       uint32
	config          WorkerConfig
	shardedConsumer shardedConsumer
}

func NewWorker(
	consumer ports.BrokerConsumer,
	events ports.EventProcessor,
	effects ports.EffectPublisher,
	batches ports.BatchProcessor,
	batchInterval time.Duration,
	clock ports.Clock,
	shardService ports.ShardService,
	workerID string,
	numShards uint32,
	config WorkerConfig,
) (*Worker, error) {
	if err := validateWorkerDependencies(consumer, events, effects, clock, shardService, workerID); err != nil {
		return nil, err
	}

	config, err := normalizeWorkerConfig(config)
	if err != nil {
		return nil, err
	}
	if numShards == 0 {
		numShards = 4096
	}

	sharded, ok := consumer.(shardedConsumer)
	if config.RequireSharded && !ok {
		return nil, errors.New("consumer must implement sharded consumer when sharded mode is enabled")
	}

	return &Worker{
		consumer:        consumer,
		events:          events,
		effects:         effects,
		batches:         batches,
		batchInterval:   batchInterval,
		clock:           clock,
		shardService:    shardService,
		workerID:        workerID,
		numShards:       numShards,
		config:          config,
		shardedConsumer: sharded,
	}, nil
}

func validateWorkerDependencies(
	consumer ports.BrokerConsumer,
	events ports.EventProcessor,
	effects ports.EffectPublisher,
	clock ports.Clock,
	shardService ports.ShardService,
	workerID string,
) error {
	switch {
	case consumer == nil:
		return errors.New("worker consumer is required")
	case events == nil:
		return errors.New("worker event processor is required")
	case effects == nil:
		return errors.New("worker effect publisher is required")
	case clock == nil:
		return errors.New("worker clock is required")
	case shardService == nil:
		return errors.New("worker shard service is required")
	case workerID == "":
		return errors.New("worker ID is required")
	default:
		return nil
	}
}

func normalizeWorkerConfig(config WorkerConfig) (WorkerConfig, error) {
	defaults := DefaultWorkerConfig()
	if config.MaxGlobalInFlight == 0 {
		config = defaults
	}
	if config.MaxGlobalInFlight < 0 {
		config.MaxGlobalInFlight = defaults.MaxGlobalInFlight
	}

	applyWorkerConfigDefaults(&config, defaults)
	if config.FetchBatchSize > config.MaxGlobalInFlight {
		return WorkerConfig{}, fmt.Errorf(
			"worker fetch batch size (%d) must not exceed max global in-flight (%d)",
			config.FetchBatchSize,
			config.MaxGlobalInFlight,
		)
	}
	return config, nil
}

func applyWorkerConfigDefaults(config *WorkerConfig, defaults WorkerConfig) {
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
}
