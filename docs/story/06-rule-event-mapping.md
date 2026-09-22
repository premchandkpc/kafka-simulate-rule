# Rule and event mapping story

This chapter connects the event stream to the rule engine and then to the durable execution and effects.

## 1. Event model

An event is the trigger. It carries the business fact that a rule will evaluate against.

The domain event is represented in [internal/domain/types.go](../../internal/domain/types.go) as `EventEnvelope`.

It contains:

- event ID
- event type
- tenant ID
- partition key
- occurred-at timestamp
- payload data

This data is the primary artifact the evaluator sees.

## 2. Rule model

A rule is a versioned object with:

- rule ID
- revision number
- content hash
- match mode
- compiled predicates
- action payloads

Rules are not mutable once active. They are immutable revisions.

This is essential because replay and audit need a stable execution identity tied to a known rule source.

## 3. Mapping from event to rule set

The mapping is simple:

```text
event.type -> rule_set
```

A rule set corresponds to an event type. In other words, a `payment.created` event may match a `payment` rule set, while a `shipment.updated` event may match a `shipment` rule set.

In the worker flow, the event type is used to find the active rule set and active revision.

## 4. Rule evaluation logic

The evaluator receives:

- the revision
- the event
- any fact snapshot

It runs predicates over the data and decides which actions should fire.

The important point is: evaluation is pure and deterministic.

It does not:

- read the database
- send HTTP requests
- mutate state
- rely on random values

This keeps replay and incident investigation deterministic.

## 5. Mapping to effects

When a condition matches, the rule emits one or more actions. These actions become effect objects.

The effect model includes:

- effect ID
- execution ID
- destination
- name
- payload
- effect type

The deterministic effect ID is computed using the tenant, event, rule set, revision, rule ID, and action index. This ensures the same logical effect is stable across retries and redelivery.

## 6. Execution audit connection

A matching rule produces an execution entry that stores:

- execution ID
- event ID
- tenant ID
- rule set
- revision
- decision hash
- status
- trace info

The execution record binds the original event and the rule decision together. That is the source of restoring a consistent audit trail.

## 7. Outbox connection

The execution record does not call destinations directly. Instead, it writes outbox effects.

This creates a clean chain:

```text
event -> rule evaluation -> execution row -> outbox rows -> effect publisher -> external destination
```

This is why the engine remains resilient to downstream failures without losing the original decision.

## 8. Why mapping matters so much

The mapping between event, rule, execution, and effect is the heart of the system. Without a strict contract, retries and replays would create duplicates, missing decisions, or impossible audits.

FlowRule’s design makes this mapping explicit:

- event ID is stable
- rule revision is pinned
- execution ID is stable
- effect ID is deterministic
- inbox prevents duplicate execution
- outbox persists effects for delivery

That is the backbone of correctness.
