package application

import (
	"context"
	"log"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/flowrule/flowrule/internal/runtime/keyqueue"
	"github.com/flowrule/flowrule/internal/runtime/shard"
)

// WorkerConfig holds configuration for the worker.
type WorkerConfig struct {
	MaxGlobalInFlight int
	MaxPerKeyQueue    int
	KeyQueueWorkers   int
}

// DefaultWorkerConfig returns sensible defaults.
func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		MaxGlobalInFlight: 100,
		MaxPerKeyQueue:    100,
		KeyQueueWorkers:   1,
	}
}

// ShardedConsumer extends BrokerConsumer with shard management.
type ShardedConsumer interface {
	EnsureShardConsumer(ctx context.Context, shard uint32) error
	RemoveShardConsumer(ctx context.Context, shard uint32) error
	FetchShards(ctx context.Context, maxMessages int, shards []uint32) ([]ports.Delivery, error)
	OwnedShards() []uint32
}

// Worker owns the runtime loop for a broker-backed rules worker.
// It separates transport bootstrap from business orchestration so the main
// entrypoint stays thin and the runtime behavior remains testable.
type Worker struct {
	consumer       ShardedConsumer
	events         ports.EventProcessor
	effects        ports.EffectPublisher
	batches        ports.BatchProcessor
	batchInterval  time.Duration
	clock          ports.Clock
	leaseManager   *shard.LeaseManager
	keyQueue       *keyqueue.KeyQueue
	workerID       string
	numShards      uint32
	config         WorkerConfig
	lastOwnedShards []uint32
}

func NewWorker(
	consumer ShardedConsumer,
	events ports.EventProcessor,
	effects ports.EffectPublisher,
	batches ports.BatchProcessor,
	batchInterval time.Duration,
	clock ports.Clock,
	leaseManager *shard.LeaseManager,
	workerID string,
	numShards uint32,
	config WorkerConfig,
) *Worker {
	if numShards == 0 {
		numShards = 4096
	}
	if config.MaxGlobalInFlight == 0 {
		config = DefaultWorkerConfig()
	}
	return &Worker{
		consumer:      consumer,
		events:        events,
		effects:       effects,
		batches:       batches,
		batchInterval: batchInterval,
		clock:         clock,
		leaseManager:  leaseManager,
		workerID:      workerID,
		numShards:     numShards,
		config:        config,
	}
}

func (w *Worker) Run(ctx context.Context) {
	if w.leaseManager != nil {
		if err := w.leaseManager.Start(ctx); err != nil {
			log.Printf("lease manager start failed: %v", err)
		}
		defer w.leaseManager.Stop(ctx)
	}

	w.keyQueue = keyqueue.NewKeyQueue(
		w.config.MaxGlobalInFlight,
		w.config.MaxPerKeyQueue,
		w.config.KeyQueueWorkers,
		func(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error {
			return w.processDelivery(ctx, delivery, fencingToken, vshard, workerID)
		},
		w.clock.Now,
	)
	w.keyQueue.Start(ctx)
	defer w.keyQueue.Stop()

	go w.publishLoop(ctx)
	if w.batches != nil && w.batchInterval > 0 {
		go w.batchLoop(ctx)
	}

	// Ensure consumers for initially owned shards
	ownedShards := w.leaseManager.OwnedShards()
	for _, shard := range ownedShards {
		if err := w.consumer.EnsureShardConsumer(ctx, shard); err != nil {
			log.Printf("ensure consumer for shard %d: %v", shard, err)
		}
	}
	w.lastOwnedShards = ownedShards

	for {
		select {
		case <-ctx.Done():
			log.Println("worker stopping")
			return
		default:
		}

		// Sync consumers with current owned shards
		w.syncConsumers(ctx)

		ownedShards := w.consumer.OwnedShards()
		if len(ownedShards) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}

		deliveries, err := w.consumer.FetchShards(ctx, 10, ownedShards)
		if err != nil {
			log.Printf("fetch: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, delivery := range deliveries {
			env, err := delivery.Event()
			if err != nil || env == nil {
				log.Printf("invalid event: %s", string(delivery.Raw()))
				if qErr := w.events.QuarantineEvent(ctx, domain.ComputeSourceHash(delivery.Raw()), "", "", domain.ErrorClassValidation, "invalid event envelope JSON"); qErr != nil {
					log.Printf("quarantine invalid event: %v", qErr)
				}
				if retryErr := delivery.Retry(ctx, 5*time.Second); retryErr != nil {
					log.Printf("retry invalid event: %v", retryErr)
				}
				continue
			}

			vshard := w.leaseManager.VirtualShardForEvent(env)
			if w.leaseManager != nil && !w.leaseManager.IsOwner(vshard) {
				if err := delivery.Nak(ctx); err != nil {
					log.Printf("nak non-owned shard %d: %v", vshard, err)
				}
				continue
			}

			fencingToken := int64(0)
			if w.leaseManager != nil {
				if token, ok := w.leaseManager.GetFencingToken(vshard); ok {
					fencingToken = token
				}
			}

			if err := w.keyQueue.Submit(ctx, env, delivery, fencingToken, vshard, w.workerID); err != nil {
				log.Printf("keyqueue submit failed: %v", err)
				if err := delivery.Nak(ctx); err != nil {
					log.Printf("nak failed: %v", err)
				}
				continue
			}
		}
	}
}

func (w *Worker) syncConsumers(ctx context.Context) {
	currentOwned := w.leaseManager.OwnedShards()

	// Add new shards
	for _, shard := range currentOwned {
		found := false
		for _, old := range w.lastOwnedShards {
			if old == shard {
				found = true
				break
			}
		}
		if !found {
			if err := w.consumer.EnsureShardConsumer(ctx, shard); err != nil {
				log.Printf("ensure consumer for shard %d: %v", shard, err)
			}
		}
	}

	// Remove old shards
	for _, shard := range w.lastOwnedShards {
		found := false
		for _, current := range currentOwned {
			if current == shard {
				found = true
				break
			}
		}
		if !found {
			if err := w.consumer.RemoveShardConsumer(ctx, shard); err != nil {
				log.Printf("remove consumer for shard %d: %v", shard, err)
			}
		}
	}

	w.lastOwnedShards = currentOwned
}

func (w *Worker) publishLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.effects.PublishBatch(ctx, 10); err != nil {
				log.Printf("publish effects: %v", err)
			}
		}
	}
}

func (w *Worker) processDelivery(ctx context.Context, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error {
	env, err := delivery.Event()
	if err != nil || env == nil {
		log.Printf("invalid event: %s", string(delivery.Raw()))
		if qErr := w.events.QuarantineEvent(ctx, domain.ComputeSourceHash(delivery.Raw()), "", "", domain.ErrorClassValidation, "invalid event envelope JSON"); qErr != nil {
			log.Printf("quarantine invalid event: %v", qErr)
		}
		if retryErr := delivery.Retry(ctx, 5*time.Second); retryErr != nil {
			log.Printf("retry invalid event: %v", retryErr)
		}
		return nil
	}

	exec, err := w.events.Process(ctx, env, fencingToken, vshard, workerID)
	if err != nil {
		log.Printf("process event %s: %v", env.ID, err)
		if domain.IsPermanent(err) {
			if qErr := w.events.QuarantineEvent(ctx, env.ID, env.ID, env.TenantID, domain.ClassifyError(err), err.Error()); qErr != nil {
				log.Printf("quarantine event %s: %v", env.ID, qErr)
			}
			if err := delivery.Ack(ctx); err != nil {
				log.Printf("ack permanent error event %s: %v", env.ID, err)
			}
			return nil
		}
		if err := delivery.Retry(ctx, 5*time.Second); err != nil {
			log.Printf("retry event %s: %v", env.ID, err)
		}
		return err
	}

	if err := delivery.Ack(ctx); err != nil {
		log.Printf("ack %s: %v", env.ID, err)
		return err
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
