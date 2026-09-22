# Example 3 — Rule evaluation patterns

## first_match mode

Rules sorted by priority DESC. Stop at first match.

### Rules

```yaml
rule_set: order.created
revision: 7
mode: first_match

rules:
  - id: high-value
    priority: 100
    when:
      path: $.data.amount
      op: gte
      value: 10000
    then:
      - emit:
          topic: orders.review
          data: { reason: high_value }

  - id: normal
    priority: 10
    when:
      path: $.data.order_id
      op: exists
    then:
      - emit:
          topic: orders.process
          data: { action: standard }
```

### Event

```json
{ "data": { "order_id": "5001", "amount": 15000 } }
```

### Trace

```text
Sorted rules: [high-value (100), normal (10)]

Evaluate high-value:
  $.data.amount >= 10000  ->  15000 >= 10000  ->  YES
  MATCH
  -> emit to orders.review
  -> BREAK (first_match)
```

Result:

```json
{
  "matched_rules": ["high-value"],
  "effects": [
    { "destination": "orders.review", "name": "orders.review" }
  ]
}
```

`normal` rule is never evaluated.

## all_matches mode

Evaluate every rule. Collect all matches.

### Rules

```yaml
mode: all_matches

rules:
  - id: high-value
    priority: 100
    when:
      path: $.data.amount
      op: gte
      value: 10000
    then:
      - emit:
          topic: fraud.review
          data: {}

  - id: indian-order
    priority: 50
    when:
      path: $.data.country
      op: eq
      value: IN
    then:
      - emit:
          topic: tax.calculate
          data: {}

  - id: gold-customer
    priority: 10
    when:
      path: $.data.customer_tier
      op: eq
      value: GOLD
    then:
      - emit:
          topic: loyalty.credit
          data: {}
```

### Event

```json
{ "data": { "amount": 15000, "country": "IN", "customer_tier": "GOLD" } }
```

### Trace

```text
Evaluate high-value:  amount >= 10000  ->  YES  -> MATCH
Evaluate indian-order: country == IN   ->  YES  -> MATCH
Evaluate gold-customer: tier == GOLD   ->  YES  -> MATCH
```

Result:

```json
{
  "matched_rules": ["high-value", "indian-order", "gold-customer"],
  "effects": [
    { "destination": "fraud.review" },
    { "destination": "tax.calculate" },
    { "destination": "loyalty.credit" }
  ]
}
```

One event produces three effects.

## otherwise actions

### Rules (first_match)

```yaml
mode: first_match

rules:
  - id: vip-check
    priority: 100
    when:
      path: $.data.customer_tier
      op: eq
      value: VIP
    then:
      - emit:
          topic: vip.process
          data: {}
    otherwise:
      - emit:
          topic: standard.process
          data: {}
```

### Event (not VIP)

```json
{ "data": { "customer_tier": "STANDARD" } }
```

### Trace

```text
Evaluate vip-check:
  customer_tier == VIP  ->  STANDARD == VIP  ->  NO
  -> fire otherwise actions
  -> emit to standard.process
```

Result:

```json
{
  "matched_rules": [],
  "effects": [
    { "destination": "standard.process" }
  ]
}
```

`otherwise` fires when the rule itself does NOT match. It is not a fallback for the entire rule set.

### Compiler constraint

In `first_match` mode, `otherwise` is only allowed on the **last-priority rule**. The compiler rejects:

```yaml
rules:
  - id: rule-a
    priority: 100
    otherwise: [...]  # REJECTED: not last priority
  - id: rule-b
    priority: 10
```

Error: `first_match permits otherwise only on the lowest-priority rule`

## Predicate operators

| Operator | Example | Meaning |
|----------|---------|---------|
| `eq` | `$.status eq "paid"` | Equal |
| `neq` | `$.status neq "cancelled"` | Not equal |
| `gt` | `$.amount gt 1000` | Greater than |
| `gte` | `$.amount gte 1000` | Greater than or equal |
| `lt` | `$.amount lt 100` | Less than |
| `lte` | `$.amount lte 100` | Less than or equal |
| `in` | `$.country in ["IN","US"]` | Value in list |
| `not_in` | `$.status not_in ["draft"]` | Value not in list |
| `exists` | `$.data.order_id exists` | Field exists |
| `not_exists` | `$.data.cancelled_at not_exists` | Field does not exist |

## Composite predicates

### all (AND)

```yaml
when:
  all:
    - path: $.amount
      op: gte
      value: 10000
    - path: $.customer_tier
      op: eq
      value: GOLD
```

All sub-predicates must be true.

### any (OR)

```yaml
when:
  any:
    - path: $.country
      op: eq
      value: IN
    - path: $.country
      op: eq
      value: US
```

At least one sub-predicate must be true.

### not (NOT)

```yaml
when:
  not:
    path: $.status
    op: eq
    value: cancelled
```

Inverts the result.

## Current implementation status

| Feature | Status |
|---------|--------|
| first_match | ✅ implemented (evaluator.go:34) |
| all_matches | ✅ implemented (evaluator.go:22-43) |
| otherwise on last rule | ✅ compiler validates (compiler.go:94-100) |
| otherwise execution | ✅ implemented (evaluator.go:38-41) |
| Priority sorting | ✅ implemented (compiler.go:172-174) |
| All operators | ✅ implemented (evaluator.go:124-143) |
| Composite predicates | ✅ implemented (evaluator.go:55-98) |
| Path resolution | ✅ implemented (evaluator.go:146-179) |
