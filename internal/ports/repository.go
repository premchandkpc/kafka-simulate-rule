package ports

import (
	"context"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
)

// Repository interfaces

// EventRepository handles event storage and retrieval
type EventRepository interface {
	Save(ctx context.Context, event *domain.EventEnvelope) error
	Get(ctx context.Context, tenantID, eventID string) (*domain.EventEnvelope, error)
	Exists(ctx context.Context, tenantID, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, tenantID, eventID, executionID string) error
	GetUnprocessed(ctx context.Context, tenantID string, limit int) ([]*domain.EventEnvelope, error)
}

// InboxRepository handles idempotency via inbox pattern
type InboxRepository interface {
	Insert(ctx context.Context, entry *domain.InboxEntry) (bool, error)
	Get(ctx context.Context, tenantID, eventID string) (*domain.InboxEntry, error)
	MarkCommitted(ctx context.Context, tenantID, eventID, executionID string) error
}

// ExecutionRepository handles execution records
type ExecutionRepository interface {
	Save(ctx context.Context, execution *domain.Execution) error
	Get(ctx context.Context, executionID string) (*domain.Execution, error)
	UpdateStatus(ctx context.Context, executionID string, status domain.ExecutionStatus, errMsg string) error
}

// OutboxRepository handles outbox pattern for reliable messaging
type OutboxRepository interface {
	Insert(ctx context.Context, effects []domain.OutboxEffect) error
	ClaimPending(ctx context.Context, batchSize int, claimant string, claimTTL time.Duration) ([]domain.OutboxEffect, error)
	MarkDelivered(ctx context.Context, effectIDs []string, claimant string) error
	ScheduleRetry(ctx context.Context, effectID string, delay time.Duration, attempts int, errMsg string) error
	Quarantine(ctx context.Context, effectID, errMsg string) error
	GetPending(ctx context.Context, limit int) ([]domain.OutboxEffect, error)
	GetByExecution(ctx context.Context, executionID string) ([]domain.OutboxEffect, error)
}

// RuleRepository handles rule storage
type RuleRepository interface {
	Save(ctx context.Context, revision *domain.RuleRevision) error
	GetActive(ctx context.Context, tenantScope, ruleSet string) (*domain.RuleRevision, error)
	Get(ctx context.Context, tenantScope, ruleSet string, revision int64) (*domain.RuleRevision, error)
	List(ctx context.Context, tenantScope string) ([]*domain.RuleRevision, error)
	Delete(ctx context.Context, tenantScope, ruleSet string, revision int64) error
}

// ActivationRepository handles active rule pointers
type ActivationRepository interface {
	Get(ctx context.Context, tenantScope, ruleSet string) (*domain.RuleActivation, error)
	Set(ctx context.Context, activation *domain.RuleActivation) error
	Delete(ctx context.Context, tenantScope, ruleSet string) error
}

// WorkflowRepository handles workflow instances
type WorkflowRepository interface {
	Save(ctx context.Context, workflow *domain.WorkflowInstance) error
	Get(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error)
	Update(ctx context.Context, workflow *domain.WorkflowInstance) error
	GetByTenantAndState(ctx context.Context, tenantID, state string, limit int) ([]*domain.WorkflowInstance, error)
	GetByCorrelation(ctx context.Context, correlationID string) ([]*domain.WorkflowInstance, error)
}

// WorkflowDefinitionRepository handles workflow definitions (templates)
type WorkflowDefinitionRepository interface {
	Save(ctx context.Context, def *domain.WorkflowDefinition) error
	Get(ctx context.Context, workflowType string, version int64) (*domain.WorkflowDefinition, error)
	List(ctx context.Context, tenantID string) ([]*domain.WorkflowDefinition, error)
	Delete(ctx context.Context, workflowType string, version int64) error
}

// ScheduledEventRepository handles scheduled/delayed events
type ScheduledEventRepository interface {
	Save(ctx context.Context, event *domain.ScheduledEvent) error
	GetDue(ctx context.Context, before time.Time, limit int) ([]*domain.ScheduledEvent, error)
	Claim(ctx context.Context, eventIDs []string, claimant string, leaseTTL time.Duration) ([]*domain.ScheduledEvent, error)
	MarkReleased(ctx context.Context, eventID string, releasedAt time.Time) error
	MarkFailed(ctx context.Context, eventID, errMsg string) error
	Get(ctx context.Context, eventID string) (*domain.ScheduledEvent, error)
}

// BatchRepository handles batch processing
type BatchRepository interface {
	SaveRun(ctx context.Context, run *domain.BatchRun) error
	GetRun(ctx context.Context, batchID string) (*domain.BatchRun, error)
	ListUnbatched(ctx context.Context, limit int) ([]*domain.InboxEntry, error)
	MarkBatched(ctx context.Context, tenantID, eventID, batchID string) error
}

// ShardLeaseRepository handles shard ownership
type ShardLeaseRepository interface {
	Acquire(ctx context.Context, shard uint32, owner string, ttl time.Duration) (*domain.ShardLease, error)
	Renew(ctx context.Context, shard uint32, owner string, fencingToken int64, ttl time.Duration) (*domain.ShardLease, error)
	Release(ctx context.Context, shard uint32, owner string) error
	GetOwner(ctx context.Context, shard uint32) (*domain.ShardLease, error)
	ValidateFencingToken(ctx context.Context, shard uint32, owner string, fencingToken int64) error
}

// QuarantineRepository handles failed items
type QuarantineRepository interface {
	Save(ctx context.Context, entry *domain.QuarantineEntry) error
	Get(ctx context.Context, id string) (*domain.QuarantineEntry, error)
	List(ctx context.Context, filter QuarantineFilter) ([]*domain.QuarantineEntry, error)
	Replay(ctx context.Context, id string) error
	Delete(ctx context.Context, id string) error
}

// QuarantineFilter for filtering quarantine entries
type QuarantineFilter struct {
	TenantID   string
	SourceType string
	ErrorClass string
	From       time.Time
	To         time.Time
	Limit      int
	Offset     int
}

// ContractRegistry handles schema contracts
type ContractRegistry interface {
	Register(ctx context.Context, schema *domain.ContractSchema) error
	Get(ctx context.Context, name, version string) (*domain.ContractSchema, error)
	List(ctx context.Context, name string) ([]*domain.ContractSchema, error)
	Delete(ctx context.Context, name, version string) error
}
