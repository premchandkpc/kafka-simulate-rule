# Rule model and correctness contract

## Design constraints

Rules are declarative, bounded, typed, and explainable. A v1 rule does not make arbitrary service calls, run loops, load code dynamically, or mutate global state. It decides which durable effects to create; separate workers deliver them.

This makes evaluation fast, replayable, and safe without a Rust VM or a second scheduling system.

## Rule shape

Use JSON or YAML first. Add a textual DSL only after the model is stable.

```yaml
id: high-value-order
version: 7
when:
  event_type: order.created
  all:
    - path: $.total
      op: gte
      value: 1000
    - path: $.country
      op: in
      value: [IN, US]
then:
  - emit:
      topic: orders.review.requested
      data:
        order_id: $.id
        reason: high_value
  - command:
      destination: risk
      name: assess-order
      data:
        order_id: $.id
otherwise:
  - emit:
      topic: orders.auto-approved
      data:
        order_id: $.id
```

## Semantics

| Item | Contract |
| --- | --- |
| Match order | Higher priority first, then stable rule ID |
| Rule set mode | `first_match` or `all_matches`, stored with the set |
| Input | Immutable event envelope plus declared read-only facts |
| Output | Ordered `Decision` of effects; no I/O during evaluation |
| Data access | Object/array paths only; no recursive queries |
| Operators | equality, comparison, membership, existence, `all`/`any`/`not` |
| Templates | Literal values and path substitutions only; no scripts |
| Limits | Maximum rule size, predicates, actions, nesting, output, and evaluation time |
| Audit | Exact revision and compiled hash recorded for every execution |

Activation validation rejects unknown schema paths, unsafe templates, duplicate effect IDs, ambiguous priorities, forbidden destinations, and limit violations. An invalid event records a typed failure and is quarantined; it is never silently dropped.

## Envelope

```json
{
  "id": "01J...",
  "type": "order.created",
  "tenant_id": "acme",
  "partition_key": "order-4821",
  "occurred_at": "2026-09-15T00:00:00Z",
  "data": {}
}
```

`id`, `tenant_id`, `type`, and `partition_key` are mandatory. A producer creates a stable ID and retries retain it. Authorization restricts each rule's tenants, event types, destinations, and output topics.

## Effects and idempotency

```text
effect_id = SHA-256(event.id + rule.revision + rule.id + action.index)
```

The outbox has a unique key on `effect_id`; the publisher sends it as the destination idempotency key. Replays therefore regenerate the same effects without duplicating business actions.

## Facts

When a decision needs current data, acquire a named, versioned fact snapshot before evaluation and record the snapshot reference or values. Do not let a rule call a live service mid-run: it makes replay nondeterministic and turns a rules engine into an unbounded workflow runtime.
