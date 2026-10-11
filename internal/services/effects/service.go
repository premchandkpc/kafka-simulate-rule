package effects

import (
	"context"
	"errors"
	"fmt"
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
	claimant     string
}

func NewService(
	outbox ports.OutboxRepository,
	effectSender ports.EffectSender,
	quarantine ports.QuarantineRepository,
	clock ports.Clock,
	claimant string,
) *Service {
	if claimant == "" {
		claimant = "publisher"
	}
	return &Service{
		outbox:       outbox,
		effectSender: effectSender,
		quarantine:   quarantine,
		clock:        clock,
		claimant:     claimant,
	}
}

// PublishBatch claims pending effects and publishes them. A delivery failure is
// persisted as a retry/quarantine transition. Persistence failures are returned
// so the caller can alert and retry instead of silently reporting success.
func (s *Service) PublishBatch(ctx context.Context, batchSize int) error {
	effects, err := s.outbox.ClaimPending(ctx, batchSize, s.claimant, 2*time.Minute)
	if err != nil {
		return fmt.Errorf("claim pending: %w", err)
	}

	var failures []error
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
		idempotencyKey := fmt.Sprintf("effect-%s", ef.ID)
		sendErr := s.effectSender.Send(ctx, effect, idempotencyKey)
		if sendErr != nil {
			attempts := ef.Attempts + 1
			if attempts >= ef.MaxAttempts {
				entry := &domain.QuarantineEntry{
					ID:         domain.NewID(),
					SourceType: "effect",
					SourceID:   ef.ID,
					ErrorClass: string(domain.ClassifyError(sendErr)),
					Error:      sendErr.Error(),
					CreatedAt:  s.clock.Now(),
				}
				if err := s.quarantine.Save(ctx, entry); err != nil {
					failures = append(failures, fmt.Errorf("save quarantine entry for effect %s: %w", ef.ID, err))
					continue
				}
				if err := s.outbox.Quarantine(ctx, ef.ID, sendErr.Error()); err != nil {
					failures = append(failures, fmt.Errorf("mark effect %s quarantined: %w", ef.ID, err))
				}
				continue
			}
			if err := s.outbox.ScheduleRetry(ctx, ef.ID, backoffDuration(attempts), attempts, sendErr.Error()); err != nil {
				failures = append(failures, fmt.Errorf("schedule retry for effect %s (send error: %v): %w", ef.ID, sendErr, err))
			}
			continue
		}

		if err := s.outbox.MarkDelivered(ctx, []string{ef.ID}, s.claimant); err != nil {
			failures = append(failures, fmt.Errorf("mark effect %s delivered: %w", ef.ID, err))
		}
	}
	return errors.Join(failures...)
}

func backoffDuration(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	if attempts > 30 {
		attempts = 30
	}
	delay := time.Duration(1<<uint(attempts-1)) * time.Second
	if delay > 60*time.Second {
		delay = 60 * time.Second
	}
	return delay
}

// Dispatch sends a single effect immediately (bypasses outbox).
func (s *Service) Dispatch(ctx context.Context, effect *domain.Effect) error {
	if effect == nil {
		return errors.New("dispatch effect: effect is nil")
	}
	return s.effectSender.Send(ctx, effect, fmt.Sprintf("effect-%s", effect.ID))
}

func (s *Service) GetPending(ctx context.Context, limit int) ([]domain.OutboxEffect, error) {
	return s.outbox.GetPending(ctx, limit)
}

func (s *Service) GetByExecution(ctx context.Context, executionID string) ([]domain.OutboxEffect, error) {
	return s.outbox.GetByExecution(ctx, executionID)
}
