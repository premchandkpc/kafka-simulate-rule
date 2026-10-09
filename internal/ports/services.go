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
}

// RuleService manages rule deployment and activation
type RuleService interface {
	CompileAndActivate(ctx context.Context, tenantScope, ruleSet string, source []byte, actor string) (*domain.RuleActivation, error)
	GetActivation(ctx context.Context, tenantScope, ruleSet string) (*domain.RuleActivation, error)
}

// EffectService manages effect dispatching
type EffectService interface {
	Dispatch(ctx context.Context, effect *domain.Effect) error
	PublishBatch(ctx context.Context, batchSize int) error
}

// WorkflowService manages long-running workflows
type WorkflowService interface {
	Get(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error)
	UpdateState(ctx context.Context, workflowID, newState string, revision int64) error
}

// BatchService manages batch processing
type BatchService interface {
	Tick(ctx context.Context) (int, error)
}

// SchedulerService manages scheduled/delayed events
type SchedulerService interface {
	Schedule(ctx context.Context, event *domain.ScheduledEvent) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// ShardService manages shard ownership
type ShardService interface {
	Acquire(ctx context.Context, shard uint32, owner string, ttl time.Duration) (*domain.ShardLease, error)
	Release(ctx context.Context, shard uint32, owner string) error
}

// QuarantineService manages failed items
type QuarantineService interface {
	Save(ctx context.Context, entry *domain.QuarantineEntry) error
	Get(ctx context.Context, id string) (*domain.QuarantineEntry, error)
	Replay(ctx context.Context, id string) error
	Delete(ctx context.Context, id string) error
}
