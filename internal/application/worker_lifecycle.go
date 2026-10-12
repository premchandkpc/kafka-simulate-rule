package application

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/flowrule/flowrule/internal/runtime/keyqueue"
)

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

