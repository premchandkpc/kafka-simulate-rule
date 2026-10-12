package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

// Service handles event processing, inbox dedup, rule evaluation, and execution creation.
type Service struct {
	compiler   ports.RuleCompiler
	evaluator  ports.RuleEvaluator
	quarantine ports.QuarantineRepository
	clock      ports.Clock
	beginTx    ports.TransactionManager
	newRepos   ports.EventTxRepositoryFactory
}

func NewService(
	compiler ports.RuleCompiler,
	evaluator ports.RuleEvaluator,
	quarantine ports.QuarantineRepository,
	clock ports.Clock,
	beginTx ports.TransactionManager,
	newRepos ports.EventTxRepositoryFactory,
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

	// F1: Reject invalid fencing token (<= 0) for all shards including 0
	if fencingToken <= 0 {
		return nil, domain.ErrFencingTokenMismatch
	}

	tx, err := s.beginTx.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// F1: Set fencing token for validation
	tx.SetFencingToken(fencingToken)

	repos, err := s.newRepos(tx)
	if err != nil {
		return nil, fmt.Errorf("create transaction repositories: %w", err)
	}
	if txCtx := tx.Context(); txCtx != nil {
		ctx = txCtx //nolint:contextcheck // tx context is intentionally used for transaction-scoped operations
	}

	// F3: Validate fencing token FIRST with row lock (AssertHeld)
	if err := repos.ShardLeases.AssertHeld(ctx, shard, workerID, fencingToken); err != nil {
		return nil, fmt.Errorf("fencing token validation failed: %w", err)
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
	// F2: Dedup fall-through - if not inserted and no execution found, it's a conflict
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
		// F2: Fall-through with nil execution = conflict
		return nil, domain.ErrInboxDuplicate
	}

	ruleSet := envelope.Type
	activation, err := repos.Activations.Get(ctx, envelope.TenantID, ruleSet)
	if err != nil {
		return nil, fmt.Errorf("get activation: %w", err)
	}
	if activation == nil {
		// Rule not yet activated for this event type. This is transient:
		// the rule may be deployed shortly after the event arrives.
		// NATS MaxDeliver + backoff bounds the retry window.
		return nil, domain.ErrRuleNotReady
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

	executionID := domain.ComputeExecutionID(envelope.TenantID, envelope.ID, ruleSet, revision.Revision)
	now := s.clock.Now()

	execution := &domain.Execution{
		ID:           executionID,
		EventID:      envelope.ID,
		TenantID:     envelope.TenantID,
		RuleSet:      ruleSet,
		Revision:     revision.Revision,
		DecisionHash: decision.Hash,
		Status:       domain.ExecutionStatusCompleted,
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
			TenantID:    envelope.TenantID,
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
	if repos.DomainServices == nil {
		return nil, fmt.Errorf("tenant domain service repository is not configured")
	}
	subscribers, err := repos.DomainServices.ListSubscribersForEvent(ctx, envelope.TenantID, envelope.Type)
	if err != nil {
		return nil, fmt.Errorf("get tenant domain subscribers: %w", err)
	}
	if len(subscribers) > 100 {
		return nil, fmt.Errorf("event %q exceeds the 100-service fan-out limit", envelope.Type)
	}
	eventPayload, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal subscribed event: %w", err)
	}
	for _, subscriber := range subscribers {
		outboxEffects = append(outboxEffects, domain.OutboxEffect{
			ID:          domain.ComputeEffectID(envelope.TenantID, envelope.ID, "service-subscription", 1, subscriber.ID, 0),
			ExecutionID: executionID,
			TenantID:    envelope.TenantID,
			Destination: subscriber.ID,
			Name:        envelope.Type,
			Payload:     eventPayload,
			EffectType:  domain.EffectTypeServiceEvent,
			Status:      domain.OutboxStatusPending,
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
