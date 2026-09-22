package application

import (
	"context"
	"log"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

// Worker owns the runtime loop for a broker-backed rules worker.
// It separates transport bootstrap from business orchestration so the main
// entrypoint stays thin and the runtime behavior remains testable.
type Worker struct {
	consumer ports.BrokerConsumer
	events   ports.EventProcessor
	effects  ports.EffectPublisher
	clock    ports.Clock
}

func NewWorker(
	consumer ports.BrokerConsumer,
	events ports.EventProcessor,
	effects ports.EffectPublisher,
	clock ports.Clock,
) *Worker {
	return &Worker{
		consumer: consumer,
		events:   events,
		effects:  effects,
		clock:    clock,
	}
}

func (w *Worker) Run(ctx context.Context) {
	go w.publishLoop(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Println("worker stopping")
			return
		default:
		}

		deliveries, err := w.consumer.Fetch(ctx, 10)
		if err != nil {
			log.Printf("fetch: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, delivery := range deliveries {
			go w.processDelivery(ctx, delivery)
		}
	}
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

func (w *Worker) processDelivery(ctx context.Context, delivery ports.Delivery) {
	env, err := delivery.Event()
	if err != nil || env == nil {
		log.Printf("invalid event: %s", string(delivery.Raw()))
		if qErr := w.events.QuarantineEvent(ctx, domain.ComputeSourceHash(delivery.Raw()), "", "", domain.ErrorClassValidation, "invalid event envelope JSON"); qErr != nil {
			log.Printf("quarantine invalid event: %v", qErr)
		}
		if retryErr := delivery.Retry(ctx, 5*time.Second); retryErr != nil {
			log.Printf("retry invalid event: %v", retryErr)
		}
		return
	}

	exec, err := w.events.Process(ctx, env)
	if err != nil {
		log.Printf("process event %s: %v", env.ID, err)
		if domain.IsPermanent(err) {
			if qErr := w.events.QuarantineEvent(ctx, env.ID, env.ID, env.TenantID, domain.ClassifyError(err), err.Error()); qErr != nil {
				log.Printf("quarantine event %s: %v", env.ID, qErr)
			}
			if err := delivery.Ack(ctx); err != nil {
				log.Printf("ack permanent error event %s: %v", env.ID, err)
			}
			return
		}
		if err := delivery.Retry(ctx, 5*time.Second); err != nil {
			log.Printf("retry event %s: %v", env.ID, err)
		}
		return
	}

	if err := delivery.Ack(ctx); err != nil {
		log.Printf("ack %s: %v", env.ID, err)
		return
	}
	log.Printf("processed event %s -> execution %s", env.ID, exec.ID)
}
