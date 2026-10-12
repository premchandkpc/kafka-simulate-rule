package application

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

const maxDeliveryRetryDelay = 5 * time.Minute

func retryDelayForDelivery(base time.Duration, delivery ports.Delivery) time.Duration {
	if base <= 0 {
		base = 5 * time.Second
	}
	attempt := uint64(1)
	if source, ok := delivery.(ports.DeliveryAttempt); ok {
		if n := source.Attempt(); n > 0 {
			attempt = n
		}
	}

	ceiling := base
	for n := uint64(1); n < attempt && ceiling < maxDeliveryRetryDelay; n++ {
		if ceiling > maxDeliveryRetryDelay/2 {
			ceiling = maxDeliveryRetryDelay
			break
		}
		ceiling *= 2
	}
	if ceiling > maxDeliveryRetryDelay {
		ceiling = maxDeliveryRetryDelay
	}

	lower := ceiling / 2
	jitterRange := int64(ceiling - lower)
	if jitterRange <= 0 {
		return ceiling
	}
	return lower + time.Duration(rand.Int64N(jitterRange+1))
}

func (w *Worker) processDelivery(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error {
	if env == nil {
		return errors.New("process delivery: event envelope is nil")
	}

	exec, err := w.events.Process(ctx, env, fencingToken, vshard, workerID)
	if err == nil && exec == nil {
		err = errors.New("event processor returned a nil execution without an error")
	}
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err != nil {
		log.Printf("process event %s: %v", env.ID, err)
		if domain.IsPermanent(err) {
			if qErr := w.events.QuarantineEvent(settleCtx, env.ID, env.ID, env.TenantID, domain.ClassifyError(err), err.Error()); qErr != nil {
				retryErr := delivery.Retry(settleCtx, retryDelayForDelivery(w.config.RetryDelay, delivery))
				return errors.Join(fmt.Errorf("quarantine permanent event %s: %w", env.ID, qErr), retryErr)
			}
			if ackErr := delivery.Ack(settleCtx); ackErr != nil {
				return fmt.Errorf("ack quarantined event %s: %w", env.ID, ackErr)
			}
			return nil
		}
		if retryErr := delivery.Retry(settleCtx, retryDelayForDelivery(w.config.RetryDelay, delivery)); retryErr != nil {
			return errors.Join(fmt.Errorf("process event %s: %w", env.ID, err), fmt.Errorf("retry event %s: %w", env.ID, retryErr))
		}
		return err
	}

	if err := delivery.Ack(settleCtx); err != nil {
		return fmt.Errorf("ack event %s: %w", env.ID, err)
	}
	log.Printf("processed event %s -> execution %s", env.ID, exec.ID)
	return nil
}

func (w *Worker) quarantineInvalidDelivery(ctx context.Context, delivery ports.Delivery, parseErr error) {
	sourceID := domain.ComputeSourceHash(delivery.Raw())
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	message := "invalid event envelope JSON"
	if parseErr != nil {
		log.Printf("invalid event envelope: subject=%q bytes=%d error=%v", delivery.Subject(), len(delivery.Raw()), parseErr)
	}
	if err := w.events.QuarantineEvent(settleCtx, sourceID, "", "", domain.ErrorClassValidation, message); err != nil {
		log.Printf("quarantine invalid event %s: %v", sourceID, err)
		if retryErr := delivery.Retry(settleCtx, retryDelayForDelivery(w.config.RetryDelay, delivery)); retryErr != nil {
			log.Printf("retry invalid event %s: %v", sourceID, retryErr)
		}
		return
	}
	if err := delivery.Ack(settleCtx); err != nil {
		log.Printf("ack quarantined invalid event %s: %v", sourceID, err)
	}
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func settleRetry(ctx context.Context, delivery ports.Delivery, delay time.Duration) error {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return delivery.Retry(settleCtx, delay)
}
