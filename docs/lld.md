# Low-level design

## Project structure

```text
cmd/api/                  HTTP/gRPC composition root and admin/query endpoints   DONE
cmd/worker/               broker fetch loop, shard ownership, graceful drain     DONE
internal/domain/          entities, value objects, domain errors; no I/O imports  DONE
internal/rules/           schema validation, compiler/index, pure evaluator      DONE
internal/application/     use cases: Activate, AcceptEvent, Evaluate, Replay     DONE
internal/ports/           narrow repository, broker, clock, and effect interfaces DONE
internal/adapters/sql/    Postgres implementation and migrations                 DONE
internal/adapters/nats/   JetStream producer/consumer implementation             DONE
internal/adapters/kafka/  optional Kafka implementation                          TODO
internal/adapters/effects/ HTTP, gRPC, and event-publish delivery adapters       DONE
internal/observability/   telemetry, health, configuration                       TODO
tests/contract/           adapter fixtures shared across implementations         DONE
tests/integration/        real SQL/broker failure tests                           DONE
migrations/               versioned schema changes                                DONE
```

Dependencies point inward: adapters depend on ports/application; application depends on domain/ports; domain depends on nothing outside the standard library. `cmd` is the only composition root. This is DDD/hexagonal architecture without turning every internal helper into an interface.

## Core types (implemented in `internal/domain/types.go`)

```go
type EventEnvelope struct {
    ID, TenantID, Type, PartitionKey string
    OccurredAt time.Time
    Data json.RawMessage
    Headers map[string]string
}

type RuleRevision struct {
    RuleID string
    Revision int64
    ContentHash string
    MatchMode FirstMatch | AllMatches
    Compiled []CompiledRule
}

type Effect struct {
    ID, ExecutionID, Destination, Name string
    Payload json.RawMessage
}

type Decision struct { Matched []string; Effects []Effect; Hash string }
```

`Evaluate(revision, event, facts) -> Decision` must be deterministic. Clock, random values, credentials, HTTP clients, and database handles cannot enter it.

## Ports (implemented in `internal/ports/ports.go`)

```go
type BrokerConsumer interface { Fetch(ctx context.Context, n int) ([]Delivery, error) }
type Delivery interface { Event() *EventEnvelope; Ack(ctx context.Context) error; Nak(ctx context.Context) error; Retry(ctx context.Context, delay time.Duration) error; Raw() []byte }
type RuleRepository interface { GetActive(ctx context.Context, tenantScope, ruleSet string) (*RuleRevision, error); Save(ctx context.Context, revision *RuleRevision) error }
type ActivationRepository interface { Get(ctx context.Context, tenantScope, ruleSet string) (*RuleActivation, error); Set(ctx context.Context, activation *RuleActivation) error }
type ExecutionRepository interface { Save(ctx context.Context, execution *Execution) error; Get(ctx context.Context, executionID string) (*Execution, error); UpdateStatus(ctx context.Context, executionID string, status ExecutionStatus, errMsg string) error }
type InboxRepository interface { Insert(ctx context.Context, entry *InboxEntry) (bool, error); Get(ctx context.Context, tenantID, eventID string) (*InboxEntry, error); MarkCommitted(ctx context.Context, tenantID, eventID, executionID string) error }
type OutboxRepository interface { Insert(ctx context.Context, effects []OutboxEffect) error; ClaimPending(ctx context.Context, batchSize int, owner string) ([]OutboxEffect, error); MarkDelivered(ctx context.Context, effectID string) error; ScheduleRetry(ctx context.Context, effectID string, delay time.Duration, attempts int, errMsg string) error; Quarantine(ctx context.Context, effectID string, errMsg string) error }
type QuarantineRepository interface { Save(ctx context.Context, entry *QuarantineEntry) error; Get(ctx context.Context, id string) (*QuarantineEntry, error); Replay(ctx context.Context, id string) error }
type ShardLeaseRepository interface { Acquire(ctx context.Context, shard uint32, owner string, ttl time.Duration) (*ShardLease, error); Renew(ctx context.Context, shard uint32, owner string, fencingToken int64, ttl time.Duration) (*ShardLease, error); Release(ctx context.Context, shard uint32, owner string) error; GetOwner(ctx context.Context, shard uint32) (*ShardLease, error) }
type EffectSender interface { Send(ctx context.Context, effect *Effect) error }
type Querier interface { Exec(...); Query(...); QueryRow(...) }
type Tx interface { Querier; Commit(ctx context.Context) error; Rollback(ctx context.Context) error }
type TxFactory func(ctx context.Context) (Tx, error)
```

`ProcessEventUseCase.Execute` owns the transaction and is intentionally one coarse use case: it atomically inserts the inbox row, pins the revision, writes the execution decision, applies owned state changes, and inserts effects. Splitting this into many repository calls invites accidental non-atomic workflows. Repositories accept a `Querier` interface (satisfied by both `*pgxpool.Pool` and `*pgx.Tx`), and a `TxFactory` + `RepoFactory` inject the transaction boundary at the composition root.

## Relational model (implemented in `migrations/`)

| Table | Primary / unique keys | Purpose |
| --- | --- | --- |
| `rule_revisions` | `(tenant_scope, rule_id, revision)` | Immutable source, compiled form, hash, validation result |
| `rule_activations` | `(tenant_scope, rule_set)` | Active revision and monotonic activation version |
| `inbox` | `unique(tenant_id, event_id)` | Final duplicate defense and processing status |
| `executions` | `execution_id` | Pinned revision, decision hash, trace, routing epoch |
| `outbox_effects` | `effect_id` | Durable effects and attempt/status metadata |
| `shard_leases` | `virtual_shard` | Owner, fencing token, expiry, routing epoch |
| `quarantine` | `quarantine_id` | Event, error category, attempts, replay audit |

Use migrations, foreign keys where retention permits, check constraints for finite status values, and an index supporting the publisher query such as `(status, available_at, created_at)`. Store large payloads in object storage with immutable content references rather than bloating hot tables.

## Transaction algorithm

```text
1. Begin transaction.
2. Insert inbox(tenant_id,event_id,status='processing').
   Unique conflict: load prior result, commit, return Duplicate.
3. Read activation and revision; record revision on execution.
4. Evaluate pure rules; validate generated effects against authorization policy.
5. Insert execution with decision hash and outbox rows with deterministic IDs.
6. Mark inbox committed and commit transaction.
7. Ack broker delivery. On any pre-commit error, roll back and retry/quarantine.
```

Choose an isolation level after concurrency tests. Default `READ COMMITTED` is sufficient if unique constraints and row locks guard the named invariant; use serializable transactions only where an aggregate invariant needs it. Keep transactions short and never include network I/O.

## Worker and publisher algorithms

Worker: obtain/renew a shard lease with a fencing token, fetch a bounded batch, dispatch only deliveries matching owned shards, process, then ack after commit. On drain, stop fetching, finish bounded in-flight work, release leases, and terminate before the platform deadline.

Publisher: claim outbox rows with `FOR UPDATE SKIP LOCKED`, send using `effect_id` as the idempotency key, then atomically mark delivered or schedule `available_at` with exponential backoff/jitter. Use a maximum attempts policy and move terminal failures to quarantine. A destination-level concurrency limiter and circuit breaker sit here.

## Rule compilation (implemented in `internal/rules/compiler.go`)

Validate JSON/YAML against a versioned schema at deployment. Resolve event-schema paths, authorize destination/topic references, enforce resource limits, and compile predicates into an index keyed by event type and candidate fields. Persist source, compiler version, compiled artifact, and content hash. Activate only a validated immutable revision.

## Observability and security

Every record and log carries `tenant_id`, `event_id`, `execution_id`, `rule_id`, `revision`, `effect_id`, `virtual_shard`, and trace context. Redact payloads by field classification. RBAC checks occur in application use cases, not only HTTP middleware. Delivery adapters resolve secret references at send time; secrets never enter rule source, execution audit, or telemetry.