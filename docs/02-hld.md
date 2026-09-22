# High-level design story

FlowRule is built around a simple guarantee: business events are processed exactly once at the domain-level, even when the broker delivers them at least once.

## 1. Core design goal

The system accepts an event, inspects the active rule revision, evaluates it in a pure deterministic manner, and records the outcome as durable state.

This is the central runtime idea expressed in the project docs and code:

- [docs/hld.md](../hld.md)
- [internal/domain/types.go](../../internal/domain/types.go)
- [internal/application/usecases.go](../../internal/application/usecases.go)

## 2. The main bounded contexts

### Rule lifecycle

Rules are stored as immutable revisions and activated atomically. Only one revision is active per tenant scope and rule set.

This ensures:

- older events keep their original rule version
- new events use the latest active rule
- audits remain traceable

### Event intake

Each event must have:

- an ID
- a type
- tenant ID
- partition key
- occurred time
- data payload

The event is validated before acceptance. The event is the immutable fact that triggers rule evaluation.

### Decisioning

Evaluation is done with a pure evaluator. It receives:

- the active rule revision
- the event envelope
- optional facts

It returns a deterministic decision plus effects, without database or network I/O.

### Effect delivery

After evaluation, the engine writes durable outbox rows. A publisher later sends those effects to destinations with retries and quarantine. This is how FlowRule avoids doing network calls inside the pure evaluation path.

## 3. Consistency model

The system uses a pragmatic consistency model:

- SQL gives atomicity for inbox and execution records
- broker provides durability and redelivery
- effects are durable in the outbox before being sent
- exact-once external delivery is only possible when the destination honors idempotency keys

This is a strong fit for business rules because it preserves correctness without pretending that all external calls can be magically atomic.

## 4. Ordering and partitioning model

Rules are processed by business key rather than by global stream order. The project uses a virtual shard derived from tenant and partition key:

```text
virtual_shard = hash(tenant_id + ':' + partition_key) mod 4096
```

The important design choice is that events for the same business entity must be ordered. Events for different keys can run in parallel.

This is why the system is scalable: a hot partition can be isolated without forcing the entire system to serialize.

## 5. Operational model

The runtime relies on a few durable facts:

1. a rule revision was activated
2. an event was accepted
3. an effect was durably committed and/or retried

Everything else is derived or operational metadata.

This keeps the system simple and auditable.

## 6. Failure handling model

The system handles failure in a layered way:

- broker redelivery if a worker fails before completion
- inbox duplicate protection to prevent double execution
- execution audit trail to preserve the original decision
- outbox retry and quarantine for downstream delivery failures

The main design principle is: never lose the event, never lose the decision, and never silently commit a partial effect.

## 7. Deployment design

FlowRule is deployed as separate services:

- API for rule and event intake
- worker pool for evaluation
- effect publisher for outbox continuity

This separation is operationally clean and allows scaling by demand. The API scales with traffic; the worker scales with backlog; the publisher scales with effect delivery rate.

## 8. Target architecture in one picture

```text
producer
  -> durable broker
  -> worker pool
  -> SQL inbox/execution/outbox
  -> outbox publisher
  -> destinations
```

The deeper architecture is already documented in:

- [docs/architecture.md](../architecture.md)
- [docs/target-plan.md](../target-plan.md)
