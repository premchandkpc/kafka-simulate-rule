# Architecture

## Project structure

```text
cmd/api/                         HTTP server for rule deployment and execution queries
cmd/worker/                      NATS consumer, event processing, effect publishing loop

internal/domain/                 Entities, value objects, domain errors. No I/O.
internal/rules/                  Schema validation, compiler, pure evaluator.
internal/services/rules/         Rule compilation, activation, querying.
internal/services/events/        Event processing, inbox dedup, evaluation, quarantine.
internal/services/effects/       Effect publishing, retry, outbox management.

internal/ports/                  Interfaces: broker, repositories, clock, services.
internal/adapters/sql/           PostgreSQL repositories, migrations, Querier wrapper.
internal/adapters/nats/          JetStream consumer and publisher.
internal/adapters/memory/        In-memory adapters for testing.
internal/adapters/effects/       HTTP and fake destination adapters.

tests/contract/                  Adapter conformance test suites.
tests/integration/               End-to-end transaction and duplicate tests.
migrations/                      Versioned schema changes.
```

## Dependency rule

```
cmd -> services -> ports
                -> domain
adapters -> ports -> domain
rules -> domain
```

Dependencies point inward. Domain depends on nothing outside the standard library. Ports define what the application needs; adapters implement it.

## Port interfaces

```go
// Transaction boundary
type Tx interface {
    Commit(ctx context.Context) error
    Rollback(ctx context.Context) error
}
type TxFactory func(ctx context.Context) (Tx, error)

// Broker
type BrokerConsumer interface {
    Fetch(ctx context.Context, n int) ([]Delivery, error)
}
type Delivery interface {
    Event() (*domain.EventEnvelope, error)
    Ack(ctx context.Context) error
    Nak(ctx context.Context) error
    Retry(ctx context.Context, delay time.Duration) error
    Raw() []byte
}

// Services (inbound ports)
type EventProcessor interface {
    Process(ctx context.Context, envelope *domain.EventEnvelope) (*domain.Execution, error)
    QuarantineEvent(ctx context.Context, sourceID, eventID, tenantID string, errClass domain.ErrorClass, errMsg string) error
}
type EffectPublisher interface {
    PublishBatch(ctx context.Context, batchSize int) error
}
type RuleCompiler interface {
    Compile(source json.RawMessage) (*domain.RuleRevision, error)
}
type RuleEvaluator interface {
    Evaluate(revision *domain.RuleRevision, event *domain.EventEnvelope, facts map[string]json.RawMessage) (*domain.Decision, error)
}
```

## Component boundaries

| Component | Does | Does not |
|-----------|------|----------|
| Event intake | Validate envelope, dedup, evaluate, record decision | Call external services, enforce global ordering |
| Rule management | Compile, validate, activate revisions | Execute rules at runtime |
| Effect publisher | Claim outbox, send, retry, quarantine | Modify execution state |
| Worker | Fetch from broker, delegate to services | Contain business logic directly |
| Broker adapter | Transport semantics (at-least-once) | Define business semantics |

## Capability matrix

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
