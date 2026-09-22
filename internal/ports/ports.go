package ports

import (
	"context"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Querier abstracts the common query interface shared by *pgxpool.Pool and *pgx.Tx.
// Both concrete types satisfy this interface, allowing repositories to operate
// inside or outside a transaction.
type Querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type BrokerConsumer interface {
	Fetch(ctx context.Context, maxMessages int) ([]Delivery, error)
}

type Delivery interface {
	Event() *domain.EventEnvelope
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

type EffectSender interface {
	Send(ctx context.Context, effect *domain.Effect) error
}

// TxFactory creates a new transaction. Injected into use cases that need
// transactional boundaries without coupling them to a specific database driver.
type TxFactory func(ctx context.Context) (Tx, error)

// Tx extends Querier with commit capability, representing an active database transaction.
type Tx interface {
	Querier
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type Clock interface {
	Now() time.Time
}

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }
