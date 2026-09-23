# Architecture

## Overview

FlowRule follows hexagonal (ports and adapters) architecture. Dependencies point inward. Domain depends on nothing outside the standard library. Ports define what the application needs; adapters implement it.

```
                           ┌─────────────────────────────────┐
                           │          cmd/api                 │
                           │    cmd/worker                    │
                           └───────────┬─────────────────────┘
                                       │
                           ┌───────────▼─────────────────────┐
                           │       internal/services          │
                           │  ┌────────┬──────────┬────────┐ │
                           │  │ rules  │ events   │ effects│ │
                           │  └────┬───┴────┬─────┴───┬────┘ │
                           └───────┼────────┼─────────┼──────┘
                                   │        │         │
                     ┌─────────────▼────────▼─────────▼──────┐
                     │           internal/ports               │
                     │   (interfaces only, no implementations)│
                     └─────────────┬─────────────────────────┘
                                   │
               ┌───────────────────┼───────────────────┐
               │                   │                   │
     ┌─────────▼─────────┐ ┌──────▼──────┐ ┌─────────▼─────────┐
     │  adapters/sql     │ │adapters/nats│ │adapters/effects   │
     │  adapters/memory  │ │             │ │                   │
     └───────────────────┘ └─────────────┘ └───────────────────┘
```

## Project Structure

```text
cmd/
  api/                         HTTP server for rule deployment and execution queries
  worker/                      NATS consumer, event processing, effect publishing loop

internal/
  domain/                      Entities, value objects, domain errors. No I/O.
  rules/                       Schema validation, compiler, pure evaluator.
  services/
    rules/                     Rule compilation, activation, querying.
    events/                    Event processing, inbox dedup, evaluation, quarantine.
    effects/                   Effect publishing, retry, outbox management.
  ports/                       Interfaces: broker, repositories, clock, services.
  adapters/
    sql/                       PostgreSQL repositories, migrations, Querier wrapper.
    nats/                      JetStream consumer and publisher.
    memory/                    In-memory adapters for testing.
    effects/                   HTTP and fake destination adapters.

tests/
  contract/                    Adapter conformance test suites.
  integration/                 End-to-end transaction and duplicate tests.
migrations/                    Versioned schema changes.
```

## Dependency Rule

```
cmd ──> services ──> ports ──> domain
                  │
adapters ─────────┘
```

| Layer | Depends on | Does not depend on |
|-------|------------|-------------------|
| domain | nothing | anything |
| ports | domain | adapters, services |
| services | ports, domain | adapters, rules internals |
| adapters | ports, domain | services, other adapters |
| cmd | services, adapters | domain internals |

## Sequence Diagrams

### Event Processing Flow

```mermaid
sequenceDiagram
    participant NATS as NATS JetStream
    participant Worker as Worker
    participant Events as EventService
    participant DB as PostgreSQL
    participant Evaluator as RuleEvaluator
    participant Outbox as OutboxRepo
    participant Publisher as EffectPublisher

    NATS->>Worker: Fetch batch (max 10)
    Worker->>Events: Process(envelope)
    Events->>DB: BEGIN TX
    Events->>DB: Inbox.Get(tenantID, eventID)
    alt Duplicate found
        Events->>DB: Execution.Get(existingID)
        Events->>DB: COMMIT
        Events-->>Worker: Return existing execution
    else New event
        Events->>DB: Inbox.Insert(processing)
        Events->>DB: Activation.Get(tenantScope, ruleSet)
        Events->>DB: RuleRepo.GetActive(tenantScope, ruleSet)
        Events->>Evaluator: Evaluate(revision, event)
        Evaluator-->>Events: Decision + Effects
        Events->>DB: Execution.Save(pending)
        Events->>DB: Outbox.Insert(effects)
        Events->>DB: Inbox.MarkCommitted(executionID)
        Events->>DB: Execution.UpdateStatus(completed)
        Events->>DB: COMMIT
        Events-->>Worker: Execution
    end
    Worker->>NATS: Ack (only after commit)
    Note over Publisher: Async publish loop
    Publisher->>DB: Outbox.ClaimPending(batch)
    Publisher->>HTTP: POST effect to destination
    alt Success
        Publisher->>DB: Outbox.MarkDelivered
    else Failure
        Publisher->>DB: Outbox.ScheduleRetry/Quarantine
    end
```

### Rule Deployment Flow

```mermaid
sequenceDiagram
    participant Client as API Client
    participant API as cmd/api
    participant Rules as RuleService
    participant Compiler as RuleCompiler
    participant DB as PostgreSQL

    Client->>API: POST /v1/rules/{ruleSet}/revisions
    API->>Rules: Activate(tenantScope, ruleSet, source, actor)
    Rules->>Compiler: Compile(source)
    Compiler-->>Rules: RuleRevision (compiled + hash)
    Rules->>DB: RuleRepo.Save(tenantScope, revision)
    Rules->>DB: Activation.Set(tenantScope, ruleSet, revision)
    Rules-->>API: RuleActivation
    API-->>Client: 200 OK + Activation
```

## Port Interfaces

### Transaction Boundary

```go
// Tx is a pure transaction abstraction. The SQL adapter wraps pgx.Tx.
type Tx interface {
    Commit(ctx context.Context) error
    Rollback(ctx context.Context) error
}

// TxFactory creates a new transaction.
type TxFactory func(ctx context.Context) (Tx, error)
```

### Broker

```go
// BrokerConsumer fetches messages from the broker.
type BrokerConsumer interface {
    Fetch(ctx context.Context, n int) ([]Delivery, error)
}

// Delivery represents a single broker message.
type Delivery interface {
    Event() (*domain.EventEnvelope, error) // Deserialize to envelope
    Ack(ctx context.Context) error         // Mark processed
    Nak(ctx context.Context) error         // Reject message
    Retry(ctx context.Context, delay time.Duration) error // Requeue with delay
    Raw() []byte                           // Raw message bytes
}
```

### Services (Inbound Ports)

```go
// EventProcessor processes incoming events through the rules engine.
type EventProcessor interface {
    Process(ctx context.Context, envelope *domain.EventEnvelope) (*domain.Execution, error)
    QuarantineEvent(ctx context.Context, sourceID, eventID, tenantID string,
        errClass domain.ErrorClass, errMsg string) error
}

// EffectPublisher publishes pending effects to their destinations.
type EffectPublisher interface {
    PublishBatch(ctx context.Context, batchSize int) error
}

// RuleCompiler compiles raw rule source into an immutable revision.
type RuleCompiler interface {
    Compile(source json.RawMessage) (*domain.RuleRevision, error)
}

// RuleEvaluator evaluates a compiled revision against an event.
type RuleEvaluator interface {
    Evaluate(revision *domain.RuleRevision, event *domain.EventEnvelope,
        facts map[string]json.RawMessage) (*domain.Decision, error)
}
```

### Repositories (Outbound Ports)

```go
type RuleRepository interface {
    GetActive(ctx context.Context, tenantScope, ruleSet string) (*domain.RuleRevision, error)
    Save(ctx context.Context, tenantScope string, revision *domain.RuleRevision) error
}

type ActivationRepository interface {
    Get(ctx context.Context, tenantScope, ruleSet string) (*domain.RuleActivation, error)
    Set(ctx context.Context, activation *domain.RuleActivation) error
}

type InboxRepository interface {
    Insert(ctx context.Context, entry *domain.InboxEntry) (bool, error)
    Get(ctx context.Context, tenantID, eventID string) (*domain.InboxEntry, error)
    MarkCommitted(ctx context.Context, tenantID, eventID, executionID string) error
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
```

### Effect Sender

```go
type EffectSender interface {
    Send(ctx context.Context, effect *domain.Effect) error
}
```

### Clock

```go
type Clock interface {
    Now() time.Time
}
```

## Component Boundaries

| Component | Does | Does Not |
|-----------|------|----------|
| Event intake (EventService) | Validate envelope, dedup, evaluate, record decision | Call external services, enforce global ordering |
| Rule management (RuleService) | Compile, validate, activate revisions | Execute rules at runtime |
| Effect publisher (EffectService) | Claim outbox, send, retry, quarantine | Modify execution state |
| Worker (application) | Fetch from broker, delegate to services | Contain business logic directly |
| Broker adapter (NATS) | Transport semantics (at-least-once) | Define business semantics |
| SQL adapter | Connection pooling, migrations, queries | Business logic |

## Capability Matrix

| Capability | FlowRule | Notes |
|------------|----------|-------|
| Rule evaluation | YES | Pure, deterministic |
| Event dedup | YES | Transactional inbox |
| Effect delivery | YES | Outbox pattern with retry |
| Rule hot-reload | YES | New revision + activate |
| Explainability | YES | RuleExplanation per evaluation |
| Contract validation | YES | InputContract on rule revisions |
| Aggregation / windows | NO | Use a workflow engine |
| Long-running workflows | NO | EmitAction chains internally; CommandAction pushes to workflow layer |
| Loops / timers / compensation | NO | Rule engine scope ends at effect emission |
| Per-key ordering | YES | Virtual shards + fencing tokens |
| Multi-tenancy | YES | tenant_scope isolation |
| Replay | PLANNED | API endpoint not yet implemented |

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| DATABASE_URL | `postgres://postgres:postgres@localhost:5432/flowrule?sslmode=disable` | PostgreSQL connection string |
| NATS_URL | `nats://localhost:4222` | NATS server URL |
| MIGRATIONS_DIR | `migrations` | Path to SQL migrations |
| API_LISTEN | `:8080` | HTTP server address |
| STREAM | `flowrule` | JetStream stream name |
| CONSUMER | `flowrule-worker` | JetStream consumer name |
| SUBJECTS | `events.>` | Subject filter for event consumption |
| ACK_WAIT | `30s` | NATS ack wait timeout |
| MAX_DELIVER | `10` | Max delivery attempts before NATS requeues |
| PUBLISH_INTERVAL | `5s` | Effect publisher batch interval |
| PUBLISH_BATCH_SIZE | `10` | Effects per publish batch |
| MAX_RULES_PER_SET | `100` | Compiler limit |
| MAX_PREDICATES | `200` | Compiler limit |
| MAX_NESTING_DEPTH | `10` | Compiler limit |
| MAX_ACTIONS_PER_SET | `10` | Compiler limit |
| MAX_PAYLOAD_BYTES | `262144` | 256KB compiler limit |

### Component Wiring (cmd/worker)

```go
// Transaction factory
beginTx := func(ctx context.Context) (ports.Tx, error) {
    pgxTx, err := pool.Begin(ctx)
    if err != nil { return nil, err }
    return sql.NewTx(pgxTx), nil
}

// Repository factory per transaction
newRepos := func(db interface{}) svcevents.TxRepos {
    q := db.(sql.Querier)
    return svcevents.TxRepos{
        Inbox:       sql.NewInboxRepository(q),
        Activations: sql.NewActivationRepository(q),
        RuleRepo:    sql.NewRuleRepository(q),
        Executions:  sql.NewExecutionRepository(q),
        Outbox:      sql.NewOutboxRepository(q),
    }
}

// Services
eventsSvc := svcevents.NewService(compiler, evaluator, quarantineRepo, clock, beginTx, newRepos)
effectsSvc := svceffects.NewService(outboxRepo, effectSender, quarantineRepo, clock)

// Worker
worker := application.NewWorker(consumer, eventsSvc, effectsSvc, clock)
```

## Testing Architecture

### Contract Tests

All adapters must pass contract tests to ensure behavioral compliance:

```go
// tests/contract/inbox_test.go
func TestInboxRepository(t *testing.T) {
    runInboxContractTests(t, func(t *testing.T) ports.InboxRepository {
        return sql.NewInboxRepository(testDB)
    })
    runInboxContractTests(t, func(t *testing.T) ports.InboxRepository {
        return memory.NewInboxRepository()
    })
}
```

### Integration Tests

End-to-end tests verify the full transaction flow:

```go
// tests/integration/event_processing_test.go
func TestEventProcessing_ExactlyOnce(t *testing.T) {
    // Start worker, publish event, verify single execution
    // Simulate crash before ack, verify no duplicate on redelivery
}
```

### Unit Tests

Pure functions tested in isolation:
- `rules/evaluator_test.go` - Predicate evaluation, effect building
- `rules/compiler_test.go` - Compilation, validation, limits
- `domain/types_test.go` - State machine transitions, ID computation