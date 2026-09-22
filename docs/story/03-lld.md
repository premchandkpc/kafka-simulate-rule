# Low-level design story

This is the implementation-level story for FlowRule.

## 1. Component map

The repo organizes the implementation in a clean hexagonal layout:

```text
cmd/
  api/
  worker/
internal/
  domain/
  rules/
  application/
  ports/
  adapters/
  observability/
```

This is visible in the repo structure and reflects the intended layering:

- domain is independent and has no external I/O dependency
- application coordinates use cases
- ports define narrow interfaces
- adapters implement SQL and broker behavior

## 2. Domain model

The domain types define the core objects of the engine:

- EventEnvelope
- RuleRevision
- CompiledRule
- Predicate
- Action
- Effect
- Decision
- Execution
- OutboxEffect
- InboxEntry
- ShardLease
- QuarantineEntry

The important implementation is in [internal/domain/types.go](../../internal/domain/types.go).

These types are deliberately simple, durable, and deterministic.

## 3. Rule compilation and validation

Rules are not entered as free-form scripts. They are validated and compiled into a structured artifact before activation.

The compiler receives rule JSON and validates:

- required ids and unique keys
- rule priority
- predicate structure
- action shape
- payload limits
- revision metadata

This ensures that only safe, deterministic rule revisions can be activated.

## 4. Usecase orchestration

The main flow in [internal/application/usecases.go](../../internal/application/usecases.go) is:

1. validate event
2. check duplicate inbox entry
3. load active revision for the tenant and rule set
4. evaluate rules
5. create execution record
6. persist outbox effects
7. mark event committed

This is the exact implementation of the durable event-processing loop.

## 5. Transaction algorithm

The real transaction boundary is intentionally narrow:

```text
begin tx
  -> insert inbox row
  -> look up active revision
  -> evaluate decision
  -> save execution
  -> insert outbox rows
  -> mark inbox committed
commit
```

This keeps the critical path short and safe. No network operation is allowed inside the same transaction boundary.

The main design principle is: durable state writes are atomic, but external effects are asynchronous and retryable.

## 6. Broker and adapter boundaries

The NATS JetStream implementation shows the intended diffusion of concerns:

- consumer fetches messages from the stream
- each message is wrapped in a delivery object
- ack/nak/retry are exposed at the port level
- the worker decides how to retry or fail

This is implemented in [internal/adapters/nats/nats.go](../../internal/adapters/nats/nats.go).

## 7. Outbox and publisher pattern

FlowRule writes effects to an outbox and publishes them later. This is the critical reliability pattern:

```text
execution commit
  -> insert outbox effects
  -> publisher claims pending rows
  -> send effect
  -> mark delivered or retry/quarantine
```

This means the business decision is never lost even if the downstream system is temporarily unavailable.

## 8. Security and observability

The design includes a disciplined approach to traceability:

- event IDs
- execution IDs
- rule revision numbers
- effect IDs
- tenant ID
- virtual shard
- trace context

This is essential for replay, quarantine, operator investigation, and incident response.

## 9. Failure model in code

The worker process [cmd/worker/main.go](../../cmd/worker/main.go) does exactly this:

- connect to Postgres and NATS
- start the fetch loop
- pull events in batches
- invoke the processing use case
- ack or retry as appropriate
- run a background publisher loop for outbox effects

The design is intentionally operationally simple and explains the architecture with minimal moving parts.
