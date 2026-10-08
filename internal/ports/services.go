package ports

import (
	"context"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
)

// EventService processes incoming events through the rules engine
type EventService interface {
	Process(ctx context.Context, envelope *domain.EventEnvelope, fencingToken int64, shard uint32, workerID string) (*domain.Execution, error)
	QuarantineEvent(ctx context.Context, sourceID, eventID, tenantID string, errClass domain.ErrorClass, errMsg string) error
	GetExecution(ctx context.Context, executionID string) (*domain.Execution, error)
	ListExecutions(ctx context.Context, filter ExecutionFilter) ([]*domain.Execution, error)
}

type ExecutionFilter struct {
	TenantID     string
	RuleSet      string
	Status       domain.ExecutionStatus
	From         time.Time
	To           time.Time
	Limit        int
	Offset       int
}

// RuleService manages rule deployment and activation
type RuleService interface {
	CompileAndActivate(ctx context.Context, tenantScope, ruleSet string, source []byte, actor string) (*domain.RuleActivation, error)
	GetActivation(ctx context.Context, tenantScope, ruleSet string) (*domain.RuleActivation, error)
	ListRevisions(ctx context.Context, tenantScope, ruleSet string) ([]*domain.RuleRevision, error)
	GetRevision(ctx context.Context, tenantScope, ruleSet string, revision int64) (*domain.RuleRevision, error)
	Deactivate(ctx context.Context, tenantScope, ruleSet string) error
	ListActiveRules(ctx context.Context, tenantScope string) ([]*domain.RuleActivation, error)
}

// EffectService manages effect dispatching
type EffectService interface {
	Dispatch(ctx context.Context, effect *domain.Effect) error
	DispatchBatch(ctx context.Context, effects []domain.Effect) error
	Retry(ctx context.Context, effectID string) error
	Quarantine(ctx context.Context, effectID, reason string) error
	GetPending(ctx context.Context, limit int) ([]*domain.OutboxEffect, error)
	GetByExecution(ctx context.Context, executionID string) ([]*domain.OutboxEffect, error)
}

// WorkflowService manages long-running workflows
type WorkflowService interface {
	Create(ctx context.Context, workflow *domain.WorkflowInstance) error
	Get(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error)
	UpdateState(ctx context.Context, workflowID, newState string, revision int64) error
	Transition(ctx context.Context, workflowID, fromState, toState string) error
	GetByTenantAndState(ctx context.Context, tenantID, state string, limit int) ([]*domain.WorkflowInstance, error)
	GetByCorrelation(ctx context.Context, correlationID string) ([]*domain.WorkflowInstance, error)
}

// BatchService manages batch processing
type BatchService interface {
	CreateBatch(ctx context.Context, tenantID, partitionKey, ruleSet string, eventIDs []string) (*domain.BatchRun, error)
	GetBatch(ctx context.Context, batchID string) (*domain.BatchRun, error)
	ListBatches(ctx context.Context, filter BatchFilter) ([]*domain.BatchRun, error)
}

type BatchFilter struct {
	TenantID     string
	PartitionKey string
	RuleSet      string
	Status       string
	From         time.Time
	To           time.Time
	Limit        int
	Offset       int
}

// SchedulerService manages scheduled/delayed events
type SchedulerService interface {
	Schedule(ctx context.Context, event *domain.ScheduledEvent) error
	Cancel(ctx context.Context, eventID string) error
	Get(ctx context.Context, eventID string) (*domain.ScheduledEvent, error)
	List(ctx context.Context, filter ScheduledEventFilter) ([]*domain.ScheduledEvent, error)
}

type ScheduledEventFilter struct {
	TenantID   string
	EventType  string
	Status     string
	From       time.Time
	To         time.Time
	Limit      int
	Offset     int
}

// ShardService manages shard ownership
type ShardService interface {
	Acquire(ctx context.Context, shard uint32, owner string, ttl time.Duration) (*domain.ShardLease, error)
	Release(ctx context.Context, shard uint32, owner string) error
	GetOwner(ctx context.Context, shard uint32) (*domain.ShardLease, error)
	ListOwned(ctx context.Context, owner string) ([]*domain.ShardLease, error)
}

// QuarantineService manages failed items
type QuarantineService interface {
	Save(ctx context.Context, entry *domain.QuarantineEntry) error
	Get(ctx context.Context, id string) (*domain.QuarantineEntry, error)
	List(ctx context.Context, filter QuarantineFilter) ([]*domain.QuarantineEntry, error)
	Replay(ctx context.Context, id string) error
	Delete(ctx context.Context, id string) error
}