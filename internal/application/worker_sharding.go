package application

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/flowrule/flowrule/internal/ports"
)

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

		// Fetch and process deliveries for owned shards. A successful empty poll
		// is idle work, not an error, but still needs a short delay to avoid a
		// hot loop while waiting for shard ownership or new messages.
		didWork, err := w.fetchAndProcessDeliveries(ctx)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				log.Printf("fetch and process error: %v", err)
			}
			if !waitForRetry(ctx, time.Second) {
				return
			}
			continue
		}
		if !didWork && !waitForRetry(ctx, 100*time.Millisecond) {
			return
		}
	}
}

// fetchAndProcessDeliveries fetches and processes deliveries for owned shards.
// It reports whether any deliveries were fetched and returns an error on fetch failure.
func (w *Worker) fetchAndProcessDeliveries(ctx context.Context) (bool, error) {
	// Fetch only shards for which this worker currently holds a lease.
	// Consumer-side removal can fail temporarily; stale consumers must not
	// remain in the fetch set while reconciliation retries.
	ownedShards := w.shardService.OwnedShards()
	if len(ownedShards) == 0 {
		return false, nil
	}

	deliveries, err := w.shardedConsumer.FetchShards(ctx, w.config.FetchBatchSize, ownedShards)
	if err != nil {
		return false, err
	}
	if len(deliveries) == 0 {
		return false, nil
	}

	for _, delivery := range deliveries {
		if err := w.processSingleDelivery(ctx, delivery); err != nil {
			log.Printf("process delivery error: %v", err)
		}
	}
	return true, nil
}

