package application

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/flowrule/flowrule/internal/rules"
)

// TxRepos holds repository instances scoped to a single transaction.
type TxRepos struct {
	Inbox       ports.InboxRepository
	Activations ports.ActivationRepository
	RuleRepo    ports.RuleRepository
	Executions  ports.ExecutionRepository
	Outbox      ports.OutboxRepository
}

// RepoFactory creates transaction-scoped repositories from a Querier.
type RepoFactory func(db ports.Querier) TxRepos

type ProcessEventUseCase struct {
	rules        *rules.Compiler
	evaluator    *rules.Evaluator
	effectSender ports.EffectSender
	clock        ports.Clock
	beginTx      ports.TxFactory
	newRepos     RepoFactory
}

func NewProcessEventUseCase(
	compiler *rules.Compiler,
	evaluator *rules.Evaluator,
	effectSender ports.EffectSender,
	clock ports.Clock,
	beginTx ports.TxFactory,
	newRepos RepoFactory,
) *ProcessEventUseCase {
	return &ProcessEventUseCase{
		rules:        compiler,
		evaluator:    evaluator,
		effectSender: effectSender,
		clock:        clock,
		beginTx:      beginTx,
		newRepos:     newRepos,
	}
}

func (uc *ProcessEventUseCase) Execute(ctx context.Context, envelope *domain.EventEnvelope) (*domain.Execution, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}

	tx, err := uc.beginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	repos := uc.newRepos(tx)

	entry, err := repos.Inbox.Get(ctx, envelope.TenantID, envelope.ID)
	if err != nil {
		return nil, fmt.Errorf("check inbox: %w", err)
	}
	if entry != nil {
		exec, err := repos.Executions.Get(ctx, entry.ExecutionID)
		if err != nil {
			return nil, fmt.Errorf("get existing execution: %w", err)
		}
		if exec != nil {
			return exec, nil
		}
	}

	inboxEntry := &domain.InboxEntry{
		TenantID:    envelope.TenantID,
		EventID:     envelope.ID,
		Status:      domain.InboxStatusProcessing,
		FirstSeenAt: uc.clock.Now(),
	}
	inserted, err := repos.Inbox.Insert(ctx, inboxEntry)
	if err != nil {
		return nil, fmt.Errorf("insert inbox: %w", err)
	}
	if !inserted {
		entry, err = repos.Inbox.Get(ctx, envelope.TenantID, envelope.ID)
		if err != nil {
			return nil, fmt.Errorf("get inbox after conflict: %w", err)
		}
		if entry != nil {
			exec, err := repos.Executions.Get(ctx, entry.ExecutionID)
			if err != nil {
				return nil, fmt.Errorf("get execution after conflict: %w", err)
			}
			if exec != nil {
				return exec, nil
			}
		}
	}

	ruleSet := envelope.Type
	activation, err := repos.Activations.Get(ctx, envelope.TenantID, ruleSet)
	if err != nil {
		return nil, fmt.Errorf("get activation: %w", err)
	}
	if activation == nil {
		return nil, domain.ErrRuleNotActive
	}

	revision, err := repos.RuleRepo.GetActive(ctx, envelope.TenantID, ruleSet)
	if err != nil {
		return nil, fmt.Errorf("get active revision: %w", err)
	}
	if revision == nil {
		return nil, domain.ErrRuleNotFound
	}

	decision, err := uc.evaluator.Evaluate(revision, envelope, nil)
	if err != nil {
		return nil, fmt.Errorf("evaluate: %w", err)
	}

	executionID := domain.NewID()
	now := uc.clock.Now()

	execution := &domain.Execution{
		ID:           executionID,
		EventID:      envelope.ID,
		TenantID:     envelope.TenantID,
		RuleSet:      ruleSet,
		Revision:     revision.Revision,
		DecisionHash: decision.Hash,
		Status:       domain.ExecutionStatusPending,
		CreatedAt:    now,
	}

	if err := repos.Executions.Save(ctx, execution); err != nil {
		return nil, fmt.Errorf("save execution: %w", err)
	}

	outboxEffects := make([]domain.OutboxEffect, 0, len(decision.Effects))
	for _, ef := range decision.Effects {
		outboxEffects = append(outboxEffects, domain.OutboxEffect{
			ID:          ef.ID,
			ExecutionID: executionID,
			Destination: ef.Destination,
			Name:        ef.Name,
			Payload:     ef.Payload,
			EffectType:  ef.EffectType,
			Status:      domain.OutboxStatusPending,
			Attempts:    0,
			MaxAttempts: 5,
			AvailableAt: now,
			CreatedAt:   now,
			UpdatedAt:   now,
		})
	}

	if err := repos.Outbox.Insert(ctx, outboxEffects); err != nil {
		return nil, fmt.Errorf("insert outbox: %w", err)
	}

	if err := repos.Inbox.MarkCommitted(ctx, envelope.TenantID, envelope.ID, executionID); err != nil {
		return nil, fmt.Errorf("mark inbox committed: %w", err)
	}

	if err := repos.Executions.UpdateStatus(ctx, executionID, domain.ExecutionStatusCompleted, ""); err != nil {
		return nil, fmt.Errorf("mark execution completed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return execution, nil
}

type PublishEffectsUseCase struct {
	outbox       ports.OutboxRepository
	effectSender ports.EffectSender
	executions   ports.ExecutionRepository
	quarantine   ports.QuarantineRepository
	clock        ports.Clock
}

func NewPublishEffectsUseCase(
	outbox ports.OutboxRepository,
	effectSender ports.EffectSender,
	executions ports.ExecutionRepository,
	quarantine ports.QuarantineRepository,
	clock ports.Clock,
) *PublishEffectsUseCase {
	return &PublishEffectsUseCase{
		outbox:       outbox,
		effectSender: effectSender,
		executions:   executions,
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

type ActivateRuleUseCase struct {
	compiler    *rules.Compiler
	ruleRepo    ports.RuleRepository
	activations ports.ActivationRepository
	clock       ports.Clock
}

func NewActivateRuleUseCase(
	compiler *rules.Compiler,
	ruleRepo ports.RuleRepository,
	activations ports.ActivationRepository,
	clock ports.Clock,
) *ActivateRuleUseCase {
	return &ActivateRuleUseCase{
		compiler:    compiler,
		ruleRepo:    ruleRepo,
		activations: activations,
		clock:       clock,
	}
}

func (uc *ActivateRuleUseCase) Execute(ctx context.Context, tenantScope string, ruleSet string, source json.RawMessage, actor string) (*domain.RuleActivation, error) {
	revision, err := uc.compiler.Compile(source)
	if err != nil {
		return nil, err
	}
	revision.RuleID = ruleSet

	if err := uc.ruleRepo.Save(ctx, tenantScope, revision); err != nil {
		return nil, fmt.Errorf("save revision: %w", err)
	}

	activation := &domain.RuleActivation{
		TenantScope: tenantScope,
		RuleSet:     ruleSet,
		Revision:    revision.Revision,
		Version:     1,
		Actor:       actor,
		ActivatedAt: uc.clock.Now(),
	}

	if err := uc.activations.Set(ctx, activation); err != nil {
		return nil, fmt.Errorf("set activation: %w", err)
	}

	return activation, nil
}
