package application

import (
	"context"
	"log"
	"time"
)

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
