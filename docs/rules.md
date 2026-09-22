# Rules

## Rule shape

```json
{
  "rule_set": "order.created",
  "revision": 1,
  "mode": "first_match",
  "rules": [{
    "id": "high-value",
    "priority": 100,
    "when": { "path": "$.total", "op": "gte", "value": 1000 },
    "then": [{ "emit": { "topic": "orders.review", "data": { "reason": "high_value" } } }]
  }]
}
```

### Fields

| Field | Required | Description |
|-------|----------|-------------|
| rule_set | yes | Maps to EventEnvelope.type |
| revision | yes | Monotonic, immutable per rule_set |
| mode | yes | `first_match` or `all_matches` |
| rules | yes | Array of rules |
| rules[].id | yes | Unique within rule_set |
| rules[].priority | yes | Higher = evaluated first |
| rules[].name | no | Human-readable name |
| rules[].description | no | What this rule does |
| rules[].tags | no | Categorization tags |
| rules[].when | yes | Predicate (leaf or composite) |
| rules[].then | yes | Array of actions |

## Predicates

### Leaf

```json
{ "path": "$.total", "op": "gte", "value": 1000 }
```

### Composite

```json
{ "all": [
    { "path": "$.country", "op": "eq", "value": "US" },
    { "any": [
        { "path": "$.total", "op": "gte", "value": 500 },
        { "path": "$.priority", "op": "eq", "value": "high" }
    ]}
]}
```

### Operators

| Op | Meaning |
|----|---------|
| eq | equals |
| neq | not equals |
| gt | greater than |
| gte | greater than or equal |
| lt | less than |
| lte | less than or equal |
| in | value in array |
| nin | value not in array |
| contains | string contains |
| starts_with | string starts with |

### Path resolution

`$.foo.bar` resolves to `event.data.foo.bar`. Dots split segments. No array indexing (`$.items[0]`) in v1.

## Actions

### Emit (internal)

```json
{ "emit": { "topic": "orders.review", "data": { "key": "$.id" } } }
```

Substitution: `"$.id"` in data resolves to `event.data.id` at evaluation time. Chains internally via broker.

### Command (workflow handoff)

```json
{ "command": { "endpoint": "workflow/create", "data": { "order_id": "$.id" } } }
```

Pushes to external workflow layer. Not executed by the rule engine.

## Evaluation modes

### first_match

Highest priority first. Stops at first match. `otherwise` allowed only on the lowest-priority rule.

### all_matches

Evaluates all rules. Collects all matched effects.

## Contracts

Rule revisions can declare an input contract:

```json
{
  "input_contract": { "name": "OrderEvent", "version": "1.0", "schema_hash": "..." }
}
```

`ValidatePathsAgainstContract` checks that all predicate paths exist in the contract schema before activation.

## Compiler limits

| Limit | Default |
|-------|---------|
| MaxRulesPerSet | 100 |
| MaxPredicates | 200 |
| MaxNestingDepth | 10 |
| MaxActionsPerSet | 10 |
| MaxPayloadBytes | 256KB |
| MaxRuleIDLen | 128 |

## Determinism

The evaluator is pure: no I/O, no clock, no randomness, no service calls. Given the same compiled revision and event, it always produces the same decision.
