package application

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
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
	if shardService == nil {
		return nil, errors.New("worker shard service is required")
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

	var sc shardedConsumer
	if candidate, ok := consumer.(shardedConsumer); ok {
		sc = candidate
	}
	if config.RequireSharded {
		if shardService == nil {
			return nil, errors.New("shard service is required when sharded mode is enabled")
		}
		if sc == nil {
			return nil, errors.New("consumer must implement sharded consumer when sharded mode is enabled")
		}
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
		shardedConsumer: sc,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("worker context is required")
	}
	if err := w.shardService.Start(ctx); err != nil {
		return fmt.Errorf("start shard service: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := w.shardService.Stop(shutdownCtx); err != nil {
			log.Printf("stop shard service: %v", err)
		}
	}()

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

	// RequireSharded defaults to true and NewWorker validates that both
	// shardedConsumer and shardService are present when enabled.
	w.runSharded(ctx)
	return nil
}

func (w *Worker) syncConsumers(ctx context.Context) {
	if w.shardService == nil || w.shardedConsumer == nil {
		return
	}

	owned := w.shardService.OwnedShards()
	ownedSet := make(map[uint32]struct{}, len(owned))
	for _, shardID := range owned {
		ownedSet[shardID] = struct{}{}
	}

	// Reconcile against the consumer's actual state every time. If removing an
	// unowned shard fails, it remains visible here and removal is retried next poll.
	active := w.shardedConsumer.OwnedShards()
	activeSet := make(map[uint32]struct{}, len(active))
	for _, shardID := range active {
		activeSet[shardID] = struct{}{}
		if _, stillOwned := ownedSet[shardID]; stillOwned {
			continue
		}
		if err := w.shardedConsumer.RemoveShardConsumer(ctx, shardID); err != nil {
			log.Printf("remove consumer for unowned shard %d: %v", shardID, err)
		} else {
			delete(activeSet, shardID)
		}
	}

	// Failed EnsureShardConsumer calls are retried because only actual consumers
	// count as active on the next synchronization pass.
	for _, shardID := range owned {
		if _, exists := activeSet[shardID]; exists {
			continue
		}
		if err := w.shardedConsumer.EnsureShardConsumer(ctx, shardID); err != nil {
			log.Printf("ensure consumer for shard %d: %v", shardID, err)
		}
	}
}

func (w *Worker) publishLoop(ctx context.Context) {
	ticker := time.NewTicker(w.config.EffectPublishInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.effects.PublishBatch(ctx, w.config.EffectPublishBatchSize); err != nil {
				log.Printf("publish effects: %v", err)
			}
		}
	}
}

const maxDeliveryRetryDelay = 5 * time.Minute

// retryDelayForDelivery applies bounded exponential backoff with equal jitter.
// Delivery attempt metadata is optional, so adapters without it safely fall
// back to the initial delay. The broker remains responsible for durable
// redelivery; this delay is not the durable retry record.
func retryDelayForDelivery(base time.Duration, delivery ports.Delivery) time.Duration {
	if base <= 0 {
		base = 5 * time.Second
	}
	attempt := uint64(1)
	if source, ok := delivery.(ports.DeliveryAttempt); ok {
		if n := source.Attempt(); n > 0 {
			attempt = n
		}
	}

	ceiling := base
	for n := uint64(1); n < attempt && ceiling < maxDeliveryRetryDelay; n++ {
		if ceiling > maxDeliveryRetryDelay/2 {
			ceiling = maxDeliveryRetryDelay
			break
		}
		ceiling *= 2
	}
	if ceiling > maxDeliveryRetryDelay {
		ceiling = maxDeliveryRetryDelay
	}

	lower := ceiling / 2
	jitterRange := int64(ceiling - lower)
	if jitterRange <= 0 {
		return ceiling
	}
	return lower + time.Duration(rand.Int64N(jitterRange+1)) //nolint:gosec // math/rand/v2 is cryptographically safe for jitter
}

func (w *Worker) processDelivery(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error {
	if env == nil {
		return errors.New("process delivery: event envelope is nil")
	}

	exec, err := w.events.Process(ctx, env, fencingToken, vshard, workerID)
	if err == nil && exec == nil {
		err = errors.New("event processor returned a nil execution without an error")
	}
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err != nil {
		log.Printf("process event %s: %v", env.ID, err)
		if domain.IsPermanent(err) {
			if qErr := w.events.QuarantineEvent(settleCtx, env.ID, env.ID, env.TenantID, domain.ClassifyError(err), err.Error()); qErr != nil {
				retryErr := delivery.Retry(settleCtx, retryDelayForDelivery(w.config.RetryDelay, delivery))
				return errors.Join(fmt.Errorf("quarantine permanent event %s: %w", env.ID, qErr), retryErr)
			}
			if ackErr := delivery.Ack(settleCtx); ackErr != nil {
				return fmt.Errorf("ack quarantined event %s: %w", env.ID, ackErr)
			}
			return nil
		}
		if retryErr := delivery.Retry(settleCtx, retryDelayForDelivery(w.config.RetryDelay, delivery)); retryErr != nil {
			return errors.Join(fmt.Errorf("process event %s: %w", env.ID, err), fmt.Errorf("retry event %s: %w", env.ID, retryErr))
		}
		return err
	}

	if err := delivery.Ack(settleCtx); err != nil {
		return fmt.Errorf("ack event %s: %w", env.ID, err)
	}
	log.Printf("processed event %s -> execution %s", env.ID, exec.ID)
	return nil
}

func (w *Worker) batchLoop(ctx context.Context) {
	interval := w.batchInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n, err := w.batches.Tick(ctx); err != nil {
				log.Printf("batch tick: %v", err)
			} else if n > 0 {
				log.Printf("batch tick formed %d batch runs", n)
			}
		}
	}
}

// runSharded runs the worker in sharded mode with lease management.
func (w *Worker) runSharded(ctx context.Context) {
	// Synchronize initial ownership through the same retryable path used later.
	w.syncConsumers(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Println("worker stopping")
			return
		default:
		}

		// Sync consumers with current owned shards
		w.syncConsumers(ctx)

		// Fetch and process deliveries for owned shards
		if err := w.fetchAndProcessDeliveries(ctx); err != nil {
			if !errors.Is(err, ctx.Err()) {
				log.Printf("fetch and process error: %v", err)
			}
			if !waitForRetry(ctx, time.Second) {
				return
			}
		}
	}
}

// fetchAndProcessDeliveries fetches and processes deliveries for owned shards.
// Returns error if fetch fails, nil if successful or no deliveries.
func (w *Worker) fetchAndProcessDeliveries(ctx context.Context) error {
	// Fetch only shards for which this worker currently holds a lease.
	// Consumer-side removal can fail temporarily; stale consumers must not
	// remain in the fetch set while reconciliation retries.
	ownedShards := w.shardService.OwnedShards()
	if len(ownedShards) == 0 {
		return nil // No shards owned, will retry
	}

	deliveries, err := w.shardedConsumer.FetchShards(ctx, w.config.FetchBatchSize, ownedShards)
	if err != nil {
		log.Printf("fetch shards: %v", err)
		return err
	}
	if len(deliveries) == 0 {
		return nil
	}

	for _, delivery := range deliveries {
		if err := w.processSingleDelivery(ctx, delivery); err != nil {
			log.Printf("process delivery error: %v", err)
		}
	}
	return nil
}

// processSingleDelivery processes a single delivery.
func (w *Worker) processSingleDelivery(ctx context.Context, delivery ports.Delivery) error {
	env, err := delivery.Event()
	if err != nil || env == nil {
		w.quarantineInvalidDelivery(ctx, delivery, err)
		return nil // Don't propagate error, delivery handled
	}

	vshard := w.shardService.VirtualShardForEvent(env)
	if !w.shardService.IsOwner(vshard) {
		if err := settleNak(ctx, delivery); err != nil {
			log.Printf("nak non-owned shard %d: %v", vshard, err)
		}
		return nil
	}

	fencingToken := int64(0)
	if token, ok := w.shardService.GetFencingToken(vshard); ok {
		fencingToken = token
	}

	if err := w.keyQueue.Submit(ctx, env, delivery, fencingToken, vshard, w.workerID); err != nil {
		log.Printf("keyqueue submit failed: %v", err)
		if err := settleNak(ctx, delivery); err != nil {
			log.Printf("nak failed: %v", err)
		}
		return err
	}
	return nil
}

// quarantineInvalidDelivery persists malformed broker messages before acknowledging
// them. If quarantine fails, the message is retried so the payload is not lost.
func (w *Worker) quarantineInvalidDelivery(ctx context.Context, delivery ports.Delivery, parseErr error) {
	sourceID := domain.ComputeSourceHash(delivery.Raw())
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	message := "invalid event envelope JSON"
	if parseErr != nil {
		log.Printf("invalid event envelope: subject=%q bytes=%d error=%v", delivery.Subject(), len(delivery.Raw()), parseErr)
	}
	if err := w.events.QuarantineEvent(settleCtx, sourceID, "", "", domain.ErrorClassValidation, message); err != nil {
		log.Printf("quarantine invalid event %s: %v", sourceID, err)
		if retryErr := delivery.Retry(settleCtx, retryDelayForDelivery(w.config.RetryDelay, delivery)); retryErr != nil {
			log.Printf("retry invalid event %s: %v", sourceID, retryErr)
		}
		return
	}
	if err := delivery.Ack(settleCtx); err != nil {
		log.Printf("ack quarantined invalid event %s: %v", sourceID, err)
	}
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func settleNak(ctx context.Context, delivery ports.Delivery) error {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return delivery.Nak(settleCtx)
}
