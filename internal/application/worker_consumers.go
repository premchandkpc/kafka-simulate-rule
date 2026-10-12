package application

import (
	"context"
	"log"
	"time"
)

func (w *Worker) syncConsumers(ctx context.Context) {
	if w.leaseManager == nil || w.shardedConsumer == nil {
		return
	}

	owned := w.leaseManager.OwnedShards()
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

		// Fetch only shards for which this worker currently holds a lease.
		// Consumer-side removal can fail temporarily; stale consumers must not
		// remain in the fetch set while reconciliation retries.
		ownedShards := w.leaseManager.OwnedShards()
		if len(ownedShards) == 0 {
			if !waitForRetry(ctx, time.Second) {
				return
			}
			continue
		}

		deliveries, err := w.shardedConsumer.FetchShards(ctx, w.config.FetchBatchSize, ownedShards)
		if err != nil {
			log.Printf("fetch shards: %v", err)
		}
		if len(deliveries) == 0 {
			delay := 100 * time.Millisecond
			if err != nil {
				delay = time.Second
			}
			if !waitForRetry(ctx, delay) {
				return
			}
			continue
		}

		for _, delivery := range deliveries {
			env, err := delivery.Event()
			if err != nil || env == nil {
				w.quarantineInvalidDelivery(ctx, delivery, err)
				continue
			}

			vshard := w.leaseManager.VirtualShardForEvent(env)
			if !w.leaseManager.IsOwner(vshard) {
				if err := settleRetry(ctx, delivery, retryDelayForDelivery(w.config.RetryDelay, delivery)); err != nil {
					log.Printf("delayed retry for non-owned shard %d: %v", vshard, err)
				}
				continue
			}

			fencingToken := int64(0)
			if token, ok := w.leaseManager.GetFencingToken(vshard); ok {
				fencingToken = token
			}

			if err := w.keyQueue.Submit(ctx, env, delivery, fencingToken, vshard, w.workerID); err != nil {
				log.Printf("keyqueue submit failed: %v", err)
				if err := settleRetry(ctx, delivery, retryDelayForDelivery(w.config.RetryDelay, delivery)); err != nil {
					log.Printf("delayed retry after queue submission failure: %v", err)
				}
				continue
			}
		}
	}
}
