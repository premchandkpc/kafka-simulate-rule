package application

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type PublishEffectsUseCase struct {
	outbox       ports.OutboxRepository
	effectSender ports.EffectSender
	quarantine   ports.QuarantineRepository
	clock        ports.Clock
}

func NewPublishEffectsUseCase(
	outbox ports.OutboxRepository,
	effectSender ports.EffectSender,
	quarantine ports.QuarantineRepository,
	clock ports.Clock,
) *PublishEffectsUseCase {
	return &PublishEffectsUseCase{
		outbox:       outbox,
		effectSender: effectSender,
		quarantine:   quarantine,
		clock:        clock,
	}
}

func (uc *PublishEffectsUseCase) Execute(ctx context.Context, batchSize int) error {
	effects, err := uc.outbox.ClaimPending(ctx, batchSize, "publisher")
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

		err := uc.effectSender.Send(ctx, effect)
		if err != nil {
			attempts := ef.Attempts + 1
			if attempts >= ef.MaxAttempts {
				quarantineErr := uc.quarantine.Save(ctx, &domain.QuarantineEntry{
					ID:         domain.NewID(),
					SourceType: "effect",
					SourceID:   ef.ID,
					ErrorClass: string(domain.ClassifyError(err)),
					Error:      err.Error(),
					CreatedAt:  uc.clock.Now(),
				})
				if quarantineErr != nil {
					log.Printf("error quarantining effect %s: %v", ef.ID, quarantineErr)
				}
				if qErr := uc.outbox.Quarantine(ctx, ef.ID, err.Error()); qErr != nil {
					log.Printf("error marking effect quarantined: %v", qErr)
				}
				continue
			}
			if sErr := uc.outbox.ScheduleRetry(ctx, ef.ID, backoffDuration(attempts), attempts, err.Error()); sErr != nil {
				log.Printf("error scheduling retry: %v", sErr)
			}
			continue
		}

		if err := uc.outbox.MarkDelivered(ctx, ef.ID); err != nil {
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
