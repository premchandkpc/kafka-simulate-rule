package ports

import (
	"context"
	"encoding/json"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
)

// Transaction represents a database transaction
type Transaction interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Context() context.Context
	FencingToken() int64
	SetFencingToken(int64)
}

// TransactionManager manages database transactions
type TransactionManager interface {
	Begin(ctx context.Context) (Transaction, error)
	WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error
}

// TxRepos holds repository instances scoped to a single transaction.
type TxRepos struct {
	Inbox           InboxRepository
	Activations     ActivationRepository
	RuleRepo        RuleRepository
	Executions      ExecutionRepository
	Outbox          OutboxRepository
	ShardLeases     ShardLeaseRepository
	Workflow        WorkflowRepository
	ScheduledEvents ScheduledEventRepository
	Batches         BatchRepository
	Quarantine      QuarantineRepository
}

// Transaction factory
type TransactionFactory func(ctx context.Context) (Transaction, error)

// Repository factory
type RepositoryFactory func(Transaction) TxRepos

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
	Process(ctx context.Context, envelope *domain.EventEnvelope, fencingToken int64, shard uint32, workerID string) (*domain.Execution, error)
	QuarantineEvent(ctx context.Context, sourceID, eventID, tenantID string, errClass domain.ErrorClass, errMsg string) error
}

// EffectPublisher publishes pending effects to their destinations.
type EffectPublisher interface {
	PublishBatch(ctx context.Context, batchSize int) error
}

// EffectSender sends effects to external destinations
type EffectSender interface {
	Send(ctx context.Context, effect *domain.Effect) error
}

// Clock provides time abstraction
type Clock interface {
	Now() time.Time
}

// Logger interface for structured logging
type Logger interface {
	Printf(format string, args ...any)
}
