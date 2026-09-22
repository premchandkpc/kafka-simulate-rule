# Producer, consumer, and request flow story

This chapter explains the live runtime flow in practical terms.

## 1. Producer side

The producer creates an event envelope that contains the business fact to process.

The event envelope includes:

- unique event ID
- type
- tenant ID
- partition key
- occurred time
- payload
- optional headers

The model is defined in [internal/domain/types.go](../../internal/domain/types.go).

A producer is expected to route events by business key, not by random round robin. The reason is simple: ordering matters per entity.

## 2. Request types

The system has a few important request categories.

### Event ingest request

The main request is an event submission. It is accepted as a durable input and published to the broker.

Flow:

```text
client -> API -> validate -> publish event -> durable stream -> worker -> execution
```

### Rule deployment request

A rule revision is submitted to the API and validated. If valid, it is saved as an immutable revision and potentially activated.

### Rule activation request

The API updates the active revision pointer for a tenant and rule set. New events use the new rule. Old executions preserve their pinned rule revision.

### Execution lookup request

A caller can inspect the execution result, decision hash, and revision used.

### Replay request

An operator can replay an event or recovery path. Replays are not random reprocessing; they are explicit and auditable.

## 3. Consumer side

The worker consumes a durable stream using a durable consumer.

The NATS implementation creates a JetStream stream and durable consumer, then fetches messages in batches.

This is implemented in [internal/adapters/nats/nats.go](../../internal/adapters/nats/nats.go), and the fetch loop is in [cmd/worker/main.go](../../cmd/worker/main.go).

The worker loop is conceptually:

```text
fetch messages
  -> deserialize event
  -> validate event
  -> ProcessEventUseCase.Execute
  -> ack if successful
  -> retry or quarantine if not
```

## 4. The actual processing request flow

The processing path is implemented in [internal/application/usecases.go](../../internal/application/usecases.go).

The flow is:

1. validate the envelope
2. check if the event already exists in inbox
3. insert an inbox row if first time
4. read the active rule activation
5. read the active rule revision
6. evaluate the event against the rule revision
7. create an execution record
8. insert outbox effects
9. mark inbox as committed
10. ack the broker delivery

This is the core runtime path of the engine.

## 5. Broker delivery semantics

The broker uses at-least-once semantics.

That means the worker must be idempotent at the application level. The project does this via:

- unique event ID
- inbox record with unique `(tenant_id, event_id)`
- execution lookup on duplicate entry
- effect IDs computed deterministically

If the message is redelivered, it will not create a new execution or a second durable effect.

## 6. Retry and backoff behavior

A bad or transient processing error is not fatal. FlowRule returns a retry signal to the broker or schedules a future retry.

The important distinction is:

- retry: transient error, safe to reattempt
- quarantine: poison message or permanent invalid condition

## 7. Publisher flow

After the execution record is written, the outbox publisher claims pending effects and sends them to destinations.

This is the effect path:

```text
outbox pending row -> effect sender -> success or failure -> delivered or retry -> quarantine
```

This keeps decision creation and action delivery separate.

## 8. Why this request model is clean

The entire system is designed around a small number of durable transitions:

- event accepted
- rule revision activated
- execution committed
- effect published

The request model is not overloaded with arbitrary workflow control. That keeps it simpler, more testable, and easier to reason about.
