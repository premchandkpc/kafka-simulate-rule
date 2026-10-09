package events

import (
	"context"
	"fmt"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

// TxRepos holds repository instances scoped to a single transaction.
type TxRepos = ports.TxRepos

// TransactionFactory creates a new transaction.
type TransactionFactory = ports.TransactionFactory

// RepositoryFactory creates transaction-scoped repositories.
type RepositoryFactory = ports.RepositoryFactory

// Service handles event processing, inbox dedup, rule evaluation, and execution creation.
type Service struct {
	compiler   ports.RuleCompiler
	evaluator  ports.RuleEvaluator
	quarantine ports.QuarantineRepository
	clock      ports.Clock
	beginTx    ports.TransactionManager
	newRepos   ports.RepositoryFactory
}

func NewService(
	compiler ports.RuleCompiler,
	evaluator ports.RuleEvaluator,
	quarantine ports.QuarantineRepository,
	clock ports.Clock,
	beginTx ports.TransactionManager,
	newRepos ports.RepositoryFactory,
) *Service {
	return &Service{
		compiler:   compiler,
		evaluator:  evaluator,
		quarantine: quarantine,
		clock:      clock,
		beginTx:    beginTx,
		newRepos:   newRepos,
	}
}

// Process processes an incoming event through the rules engine.
func (s *Service) Process(ctx context.Context, envelope *domain.EventEnvelope, fencingToken int64, shard uint32, workerID string) (*domain.Execution, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}

	tx, err := s.beginTx.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if fencingToken > 0 {
		tx.SetFencingToken(fencingToken)
	}

	repos, err := s.newRepos(tx)
	if err != nil {
		return nil, fmt.Errorf("create transaction repositories: %w", err)
	}
	if txCtx := tx.Context(); txCtx != nil {
		ctx = txCtx
	}

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
		TenantID:     envelope.TenantID,
		EventID:      envelope.ID,
		Status:       domain.InboxStatusProcessing,
		PartitionKey: envelope.PartitionKey,
		RuleSet:      envelope.Type,
		Payload:      envelope.Data,
		FirstSeenAt:  s.clock.Now(),
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

	decision, err := s.evaluator.Evaluate(revision, envelope, nil)
	if err != nil {
		return nil, fmt.Errorf("evaluate: %w", err)
	}

	executionID := domain.NewID()
	now := s.clock.Now()

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

	if fencingToken > 0 && shard > 0 && workerID != "" {
		if err := repos.ShardLeases.ValidateFencingToken(ctx, shard, workerID, fencingToken); err != nil {
			return nil, fmt.Errorf("fencing token validation failed: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return execution, nil
}

// QuarantineEvent records a failed event for later inspection.
func (s *Service) QuarantineEvent(ctx context.Context, sourceID string, eventID string, tenantID string, errClass domain.ErrorClass, errMsg string) error {
	return s.quarantine.Save(ctx, &domain.QuarantineEntry{
		ID:         domain.NewID(),
		SourceType: "event",
		SourceID:   sourceID,
		EventID:    eventID,
		TenantID:   tenantID,
		ErrorClass: string(errClass),
		PayloadRef: "jetstream:flowrule",
		Error:      errMsg,
		CreatedAt:  s.clock.Now(),
	})
}
