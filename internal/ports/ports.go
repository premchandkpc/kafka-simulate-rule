package ports

import (
	"context"
	"encoding/json"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
)

type BrokerConsumer interface {
	Fetch(ctx context.Context, maxMessages int) ([]Delivery, error)
}

type Delivery interface {
	Event() (*domain.EventEnvelope, error)
	Ack(ctx context.Context) error
	Nak(ctx context.Context) error
	Retry(ctx context.Context, delay time.Duration) error
	Raw() []byte
}

type BrokerPublisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

type RuleRepository interface {
	GetActive(ctx context.Context, tenantScope string, ruleSet string) (*domain.RuleRevision, error)
	Save(ctx context.Context, tenantScope string, revision *domain.RuleRevision) error
}

type ActivationRepository interface {
	Get(ctx context.Context, tenantScope string, ruleSet string) (*domain.RuleActivation, error)
	Set(ctx context.Context, activation *domain.RuleActivation) error
}

type InboxRepository interface {
	Insert(ctx context.Context, entry *domain.InboxEntry) (bool, error)
	Get(ctx context.Context, tenantID string, eventID string) (*domain.InboxEntry, error)
	MarkCommitted(ctx context.Context, tenantID string, eventID string, executionID string) error
}

type ExecutionRepository interface {
	Save(ctx context.Context, execution *domain.Execution) error
	Get(ctx context.Context, executionID string) (*domain.Execution, error)
	UpdateStatus(ctx context.Context, executionID string, status domain.ExecutionStatus, errMsg string) error
}

type OutboxRepository interface {
	Insert(ctx context.Context, effects []domain.OutboxEffect) error
	ClaimPending(ctx context.Context, batchSize int, owner string) ([]domain.OutboxEffect, error)
	MarkDelivered(ctx context.Context, effectID string) error
	ScheduleRetry(ctx context.Context, effectID string, availableAt time.Duration, attempts int, errMsg string) error
	Quarantine(ctx context.Context, effectID string, errMsg string) error
}

type QuarantineRepository interface {
	Save(ctx context.Context, entry *domain.QuarantineEntry) error
	Get(ctx context.Context, id string) (*domain.QuarantineEntry, error)
	Replay(ctx context.Context, id string) error
}

type ShardLeaseRepository interface {
	Acquire(ctx context.Context, shard uint32, owner string, ttl time.Duration) (*domain.ShardLease, error)
	Renew(ctx context.Context, shard uint32, owner string, fencingToken int64, ttl time.Duration) (*domain.ShardLease, error)
	Release(ctx context.Context, shard uint32, owner string) error
	GetOwner(ctx context.Context, shard uint32) (*domain.ShardLease, error)
}

type ContractRegistry interface {
	Get(ctx context.Context, name string, version string) (*domain.ContractSchema, error)
	Register(ctx context.Context, schema *domain.ContractSchema) error
	List(ctx context.Context, name string) ([]domain.ContractSchema, error)
}

type EffectSender interface {
	Send(ctx context.Context, effect *domain.Effect) error
}

type Logger interface {
	Printf(format string, args ...any)
}

type Clock interface {
	Now() time.Time
}

// Tx is a pure transaction abstraction. The SQL adapter wraps pgx.Tx
// and implements this interface for the application layer.
type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// TxFactory creates a new transaction.
type TxFactory func(ctx context.Context) (Tx, error)

// RuleCompiler compiles raw rule source into an immutable revision.
type RuleCompiler interface {
	Compile(source json.RawMessage) (*domain.RuleRevision, error)
}

// RuleEvaluator evaluates a compiled revision against an event.
type RuleEvaluator interface {
	Evaluate(revision *domain.RuleRevision, event *domain.EventEnvelope, facts map[string]json.RawMessage) (*domain.Decision, error)
}

// EventProcessor processes incoming events through the rules engine.
type EventProcessor interface {
	Process(ctx context.Context, envelope *domain.EventEnvelope) (*domain.Execution, error)
	QuarantineEvent(ctx context.Context, sourceID string, eventID string, tenantID string, errClass domain.ErrorClass, errMsg string) error
}

// EffectPublisher publishes pending effects to their destinations.
type EffectPublisher interface {
	PublishBatch(ctx context.Context, batchSize int) error
}
