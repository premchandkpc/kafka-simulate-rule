# Rule Runtime Contract

## V1 scope

Rules are declarative, typed, bounded, deterministic, and explainable. JSON is the canonical wire representation; YAML is an authoring format that is converted to the same model. A textual DSL may be added later as a compiler front end, not as a second semantic engine.

A rule can:

- match event type and typed paths;
- combine `all`, `any`, and `not` predicates;
- read literal values and declared fact snapshot paths;
- create an ordered list of `emit` or `command` effects;
- select `first_match` or `all_matches` behavior.

A rule cannot perform I/O, call a service, sleep, read a clock, generate randomness, loop, load code, mutate global state, or write directly to a database.

## Canonical shape

```yaml
rule_set: order-policy
revision: 7
mode: first_match
rules:
  - id: high-value
    priority: 100
    when:
      all:
        - path: $.type
          op: eq
          value: order.created
        - path: $.data.total
          op: gte
          value: 1000
    then:
      - command:
          destination: risk
          name: assess-order
          data:
            order_id: $.data.id
            reason: high_value
```

Activation rejects unknown operators, unsafe paths, duplicate IDs, ambiguous priorities, unauthorized destinations, and limit violations. The compiler stores source hash, compiler version, normalized artifact, and artifact hash.

## Determinism

`Evaluate(revision, event, facts) -> decision` is pure. A decision hash is calculated from canonical serialization of revision hash, event ID, facts snapshot ID, matches, and ordered effects.

The event envelope contains a stable producer-created ID. Facts are an explicit immutable snapshot with a version or content hash. A live lookup inside evaluation is forbidden because it makes replay and incident analysis nondeterministic.

## Effect identity

```text
effect_id = SHA-256(tenant_id || event_id || rule_set || revision || rule_id || action_index)
```

Include destination and schema version in the canonical action identity if the same action position can change meaning. Replays of the same event and revision must regenerate the same effect IDs. A deliberate new business attempt receives a new event ID and is auditable as a new operation.

## Limits

Set and enforce limits before activation and at runtime: rule bytes, rules per set, predicate count, nesting depth, action count, payload bytes, fact bytes, evaluation CPU/time, and effects per event. Limits prevent denial of service and make capacity planning possible.

## Errors

- Invalid envelope: reject at ingress.
- Invalid rule: reject before activation.
- Evaluation type/limit error: record a terminal execution failure and quarantine the event.
- Destination transient failure: retry the outbox effect.
- Destination permanent failure: quarantine the effect with a reconciliation path.

Errors are typed and stable for metrics and APIs. Raw payloads and secrets are not placed in logs.
