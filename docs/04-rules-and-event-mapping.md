# Story 04 — Rules and event mapping

## Mapping contract

An event is mapped to a rule set by **exact string equality**:

```text
rule_set = EventEnvelope.type
activation = (EventEnvelope.tenant_id, rule_set)
```

This is the core contract: the producer says what happened, and FlowRule decides what to do about it.
A producer should emit an event type like `order.created`, not a private internal rule name such as `Rule-101`.

```text
Event type: "order.created"
        ↓
Rule set: "order.created"
        ↓
Active revision: 7
        ↓
Rules: Rule 101, 102, 103
        ↓
Decision / effects
```

The NATS subject routes delivery to the worker but does not select a rule. All `events.>` messages share one durable consumer in the current worker; a subject such as `events.order-created` is simply a transport convention.

## Event envelope

`api/envelope-schema.json` defines the wire schema. Required fields are `id`, `type`, `tenant_id`, `partition_key`, `occurred_at`, and object `data`; `headers` is optional. Runtime validation currently checks only that the required fields are non-empty/non-zero—not JSON-schema lengths, regexes, or the object shape—so validate producer payloads at ingress too.

Example:

```json
{
  "id": "evt_001",
  "type": "order-created",
  "tenant_id": "acme",
  "partition_key": "order-123",
  "occurred_at": "2026-09-22T10:00:00Z",
  "data": {"total": 1200, "country": "IN"},
  "headers": {"traceparent": "00-..."}
}
```

## Rule document and evaluation

A rule document has a `rule_set`, a positive `revision`, mode `first_match` or `all_matches`, and 1–100 uniquely identified rules. Rules have unique numeric priorities and are sorted high-to-low. A `when` expression can use `all`, `any`, `not`, or a leaf predicate.

| Leaf operator | Meaning |
| --- | --- |
| `eq`, `neq` | Value equality/inequality |
| `gt`, `gte`, `lt`, `lte` | Numeric comparison when both values parse as numbers; otherwise lexical string comparison |
| `in`, `not_in` | Membership in an array literal |
| `exists`, `not_exists` | JSON path present/absent |

Paths are evaluated against `event.data`, e.g. `$.total` or `$.customer.tier`. Despite the schema allowing array indexes, the current resolver splits only on dots and does not implement `$.items[0]`; facts are accepted by the evaluator signature but are not consulted. Those are implementation constraints, not supported behavior.

For a matching rule, every `then` action becomes an effect. For a nonmatching rule, every `otherwise` action becomes an effect. In `first_match` mode evaluation stops after the first match; preceding nonmatches may already have emitted `otherwise` effects. In `all_matches`, every rule is visited. This detail is important when authoring fallbacks.

## Actions and effects

| Rule action | Persisted effect type | Destination/name |
| --- | --- | --- |
| `emit: {topic, data}` | `emit` | both destination and name are `topic` |
| `command: {destination, name, data}` | `command` | provided destination and name |

Actions use static JSON data. The engine does not template data with event fields, call external facts, or mutate state during evaluation. The resulting decision hash covers revision content hash, event ID, matched rule IDs, and effects.

Keep revisions immutable. A retry must use the revision captured in its execution record, rather than reselecting today’s activation. The execution record stores the revision used at evaluation time, and the transactional inbox ensures that a crash at any point rolls back cleanly — redelivery either finds no execution (re-evaluates with the current activation) or finds the existing execution (returns it with the pinned revision).

## Rule engine boundary vs workflow layer

The current architecture explicitly treats FlowRule as a rules engine and not a general workflow engine.

This means:

- a single event with multiple rules is normal
- sequential rule chains should usually be modeled as event chaining, not internal recursive rule calls
- parallel rules are natural when they decide independent effects off the same event
- aggregation, state correlation, batch windows, human approval, and compensation should live in a separate aggregate or workflow layer

A clean call pattern is:

```text
Event A
  -> Rule Set A
  -> emit Event B
  -> broker
  -> Rule Set B
  -> emit Event C
  -> broker
```

This preserves determinism, replayability, partition ordering, and effect durability while keeping the rules engine small and debuggable.
