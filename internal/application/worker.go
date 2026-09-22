package application

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

// EventProcessor processes incoming events.
type EventProcessor interface {
	Process(ctx context.Context, envelope *domain.EventEnvelope) (*domain.Execution, error)
}

// EffectPublisher publishes pending effects.
type EffectPublisher interface {
	PublishBatch(ctx context.Context, batchSize int) error
}

// Worker owns the runtime loop for a broker-backed rules worker.
// It separates transport bootstrap from business orchestration so the main
// entrypoint stays thin and the runtime behavior remains testable.
type Worker struct {
	consumer   ports.BrokerConsumer
	events     EventProcessor
	effects    EffectPublisher
	quarantine ports.QuarantineRepository
	clock      ports.Clock
}

func NewWorker(
	consumer ports.BrokerConsumer,
	events EventProcessor,
	effects EffectPublisher,
	quarantine ports.QuarantineRepository,
	clock ports.Clock,
) *Worker {
	return &Worker{
		consumer:   consumer,
		events:     events,
		effects:    effects,
		quarantine: quarantine,
		clock:      clock,
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
		data, _ := json.Marshal(delivery.Raw())
		log.Printf("invalid event: %s", string(data))
		if err := w.quarantine.Save(ctx, &domain.QuarantineEntry{
			ID:         domain.NewID(),
			SourceType: "event",
			SourceID:   domain.ComputeSourceHash(delivery.Raw()),
			ErrorClass: string(domain.ErrorClassValidation),
			PayloadRef: "jetstream:flowrule",
			Error:      "invalid event envelope JSON",
			CreatedAt:  w.clock.Now(),
		}); err != nil {
			log.Printf("quarantine invalid event: %v", err)
			if retryErr := delivery.Retry(ctx, 5*time.Second); retryErr != nil {
				log.Printf("retry invalid event: %v", retryErr)
			}
			return
		}
		if err := delivery.Ack(ctx); err != nil {
			log.Printf("ack invalid event: %v", err)
		}
		return
	}

	exec, err := w.events.Process(ctx, env)
	if err != nil {
		log.Printf("process event %s: %v", env.ID, err)
		if domain.IsPermanent(err) {
			if qErr := w.quarantine.Save(ctx, &domain.QuarantineEntry{
				ID:         domain.NewID(),
				SourceType: "event",
				SourceID:   env.ID,
				EventID:    env.ID,
				TenantID:   env.TenantID,
				ErrorClass: string(domain.ClassifyError(err)),
				PayloadRef: "jetstream:flowrule",
				Error:      err.Error(),
				CreatedAt:  w.clock.Now(),
			}); qErr != nil {
				log.Printf("quarantine event %s: %v", env.ID, qErr)
				if retryErr := delivery.Retry(ctx, 5*time.Second); retryErr != nil {
					log.Printf("retry quarantined event: %v", retryErr)
				}
				return
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
