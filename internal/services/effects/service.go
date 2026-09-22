package effects

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

// Service handles effect publishing, retries, and quarantine.
type Service struct {
	outbox       ports.OutboxRepository
	effectSender ports.EffectSender
	quarantine   ports.QuarantineRepository
	clock        ports.Clock
}

func NewService(
	outbox ports.OutboxRepository,
	effectSender ports.EffectSender,
	quarantine ports.QuarantineRepository,
	clock ports.Clock,
) *Service {
	return &Service{
		outbox:       outbox,
		effectSender: effectSender,
		quarantine:   quarantine,
		clock:        clock,
	}
}

// PublishBatch claims pending effects and publishes them.
func (s *Service) PublishBatch(ctx context.Context, batchSize int) error {
	effects, err := s.outbox.ClaimPending(ctx, batchSize, "publisher")
	if err != nil {
		return fmt.Errorf("claim pending: %w", err)
	}

	for _, ef := range effects {
		effect := &domain.Effect{
			ID:          ef.ID,
			ExecutionID: ef.ExecutionID,
			Destination: ef.Destination,
			Name:        ef.Name,
			Payload:     ef.Payload,
			EffectType:  ef.EffectType,
			CreatedAt:   ef.CreatedAt,
		}

		err := s.effectSender.Send(ctx, effect)
		if err != nil {
			attempts := ef.Attempts + 1
			if attempts >= ef.MaxAttempts {
				quarantineErr := s.quarantine.Save(ctx, &domain.QuarantineEntry{
					ID:         domain.NewID(),
					SourceType: "effect",
					SourceID:   ef.ID,
					ErrorClass: string(domain.ClassifyError(err)),
					Error:      err.Error(),
					CreatedAt:  s.clock.Now(),
				})
				if quarantineErr != nil {
					log.Printf("error quarantining effect %s: %v", ef.ID, quarantineErr)
				}
				if qErr := s.outbox.Quarantine(ctx, ef.ID, err.Error()); qErr != nil {
					log.Printf("error marking effect quarantined: %v", qErr)
				}
				continue
			}
			if sErr := s.outbox.ScheduleRetry(ctx, ef.ID, backoffDuration(attempts), attempts, err.Error()); sErr != nil {
				log.Printf("error scheduling retry: %v", sErr)
			}
			continue
		}

		if err := s.outbox.MarkDelivered(ctx, ef.ID); err != nil {
			log.Printf("error marking delivered: %v", err)
		}
	}

	return nil
}

func backoffDuration(attempts int) time.Duration {
	delay := time.Duration(1<<uint(attempts-1)) * time.Second
	if delay > 60*time.Second {
		delay = 60 * time.Second
	}
	return delay
}
