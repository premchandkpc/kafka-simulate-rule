# Rule and Event Model

## Event Envelope

```json
{
  "id": "01J8K5M3N2P1R0S7T9V2W4X6Y8Z",
  "type": "order.created",
  "tenant_id": "acme",
  "partition_key": "order-4821",
  "occurred_at": "2026-09-21T10:30:00Z",
  "data": {
    "id": "order-4821",
    "total": 1500,
    "country": "US",
    "items": [
      {"sku": "WIDGET-1", "qty": 2, "price": 750}
    ]
  },
  "headers": {
    "source": "order-service",
    "trace-id": "abc-123"
  }
}
```

### Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `id` | string | yes | Producer-created stable ID. Retries retain it. |
| `type` | string | yes | Event type. Maps to rule_set name. |
| `tenant_id` | string | yes | Tenant isolation key. |
| `partition_key` | string | yes | Business key for ordering (order ID, account ID). |
| `occurred_at` | timestamp | yes | When the event occurred (UTC RFC 3339). |
| `data` | object | yes | Event payload. Accessed by rules via `$.field`. |
| `headers` | map[string]string | no | Metadata for transport/routing. |

### Virtual Shard

```text
virtual_shard = hash(tenant_id + ":" + partition_key) mod 4096
```

Events with the same `partition_key` land on the same worker. Different keys execute concurrently.

---

## Rule Set

```json
{
  "rule_set": "order.created",
  "revision": 7,
  "mode": "first_match",
  "rules": [
    {
      "id": "high-value",
      "priority": 100,
      "when": {
        "all": [
          {"path": "$.total", "op": "gte", "value": 1000},
          {"path": "$.country", "op": "in", "value": ["IN", "US"]}
        ]
      },
      "then": [
        {
          "emit": {
            "topic": "orders.review.requested",
            "data": {"order_id": "$.id", "reason": "high_value"}
          }
        },
        {
          "command": {
            "destination": "risk-service",
            "name": "assess-order",
            "data": {"order_id": "$.id"}
          }
        }
      ],
      "otherwise": [
        {
          "emit": {
            "topic": "orders.auto-approved",
            "data": {"order_id": "$.id"}
          }
        }
      ]
    },
    {
      "id": "fraud-check",
      "priority": 50,
      "when": {
        "path": "$.total",
        "op": "gt",
        "value": 5000
      },
      "then": [
        {
          "command": {
            "destination": "fraud-service",
            "name": "investigate",
            "data": {"order_id": "$.id"}
          }
        }
      ]
    }
  ]
}
```

### Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `rule_set` | string | yes | Name. Matches event `type`. |
| `revision` | int64 | yes | Version number. Immutable after publish. |
| `mode` | string | yes | `first_match` or `all_matches`. |
| `rules` | array | yes | Rules sorted by priority DESC. |

### Rule Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `id` | string | yes | Unique within rule_set. |
| `priority` | int | yes | Higher = evaluated first. Must be unique. |
| `when` | predicate | yes | Condition to match. |
| `then` | array | yes | Actions if match. |
| `otherwise` | array | no | Actions if no match. |

---

## Predicate

### Leaf Predicate

```json
{"path": "$.total", "op": "gte", "value": 1000}
```

| Field | Type | Description |
|---|---|---|
| `path` | string | JSONPath starting with `$.` (e.g., `$.data.total`) |
| `op` | string | Operator (see below) |
| `value` | any | Comparison value |

### Operators

| Operator | Description | Example |
|---|---|---|
| `eq` | Equal | `{"path": "$.status", "op": "eq", "value": "active"}` |
| `neq` | Not equal | `{"path": "$.status", "op": "neq", "value": "deleted"}` |
| `gt` | Greater than | `{"path": "$.total", "op": "gt", "value": 100}` |
| `gte` | Greater than or equal | `{"path": "$.total", "op": "gte", "value": 100}` |
| `lt` | Less than | `{"path": "$.count", "op": "lt", "value": 10}` |
| `lte` | Less than or equal | `{"path": "$.count", "op": "lte", "value": 10}` |
| `in` | In list | `{"path": "$.country", "op": "in", "value": ["US", "IN"]}` |
| `not_in` | Not in list | `{"path": "$.country", "op": "not_in", "value": ["CN"]}` |
| `exists` | Field exists | `{"path": "$.email", "op": "exists"}` |
| `not_exists` | Field does not exist | `{"path": "$.deleted_at", "op": "not_exists"}` |

### Composite Predicates

#### all (AND)
```json
{
  "all": [
    {"path": "$.total", "op": "gte", "value": 1000},
    {"path": "$.country", "op": "eq", "value": "US"}
  ]
}
```

#### any (OR)
```json
{
  "any": [
    {"path": "$.total", "op": "gte", "value": 1000},
    {"path": "$.priority", "op": "eq", "value": "high"}
  ]
}
```

#### not (NEGATE)
```json
{
  "not": {"path": "$.status", "op": "eq", "value": "cancelled"}
}
```

### Nesting

```json
{
  "all": [
    {"path": "$.total", "op": "gte", "value": 1000},
    {
      "any": [
        {"path": "$.country", "op": "eq", "value": "US"},
        {"path": "$.country", "op": "eq", "value": "IN"}
      ]
    },
    {
      "not": {"path": "$.status", "op": "eq", "value": "cancelled"}
    }
  ]
}
```

---

## Actions

### Emit Action

Publishes an event to a NATS subject or Kafka topic.

```json
{
  "emit": {
    "topic": "orders.review.requested",
    "data": {
      "order_id": "$.id",
      "reason": "high_value"
    }
  }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `topic` | string | yes | Subject/topic name |
| `data` | object | yes | Payload with path substitutions |

### Command Action

Sends a command to an external service.

```json
{
  "command": {
    "destination": "risk-service",
    "name": "assess-order",
    "data": {
      "order_id": "$.id",
      "amount": "$.total"
    }
  }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `destination` | string | yes | Service name |
| `name` | string | yes | Command name |
| `data` | object | yes | Payload with path substitutions |

---

## Path Resolution

Paths use JSONPath syntax starting with `$`:

| Path | Resolves To |
|---|---|
| `$.id` | `event.data.id` |
| `$.total` | `event.data.total` |
| `$.country` | `event.data.country` |
| `$.items` | `event.data.items` (array) |

### Substitutions in Actions

In action data, paths are strings that get resolved:

```json
{
  "data": {
    "order_id": "$.id",
    "amount": "$.total",
    "static_value": "hello"
  }
}
```

Becomes:

```json
{
  "data": {
    "order_id": "order-4821",
    "amount": 1500,
    "static_value": "hello"
  }
}
```

---

## Execution Flow

```text
Event arrives
  |
  v
Match event.type to rule_set
  |
  v
Load active revision for (tenant_id, rule_set)
  |
  v
For each rule (sorted by priority DESC):
  |
  | Evaluate when predicate
  |   - Walk path in event.data
  |   - Apply operator
  |   - Combine with all/any/not
  |
  | If match (or first_match mode):
  |   - Execute Then actions
  |   - Generate effect_id = SHA-256(tenant | event | rule_set | revision | rule_id | index)
  |   - Store in outbox
  |
  | If no match and Otherwise exists:
  |   - Execute Otherwise actions
  |
  v
Return Decision { matched_rules, effects, hash }
```

---

## Example: Complete Rule Evaluation

### Event

```json
{
  "id": "evt-001",
  "type": "order.created",
  "tenant_id": "acme",
  "partition_key": "order-4821",
  "data": {"id": "order-4821", "total": 1500, "country": "US"}
}
```

### Rule Set

```json
{
  "rule_set": "order.created",
  "revision": 1,
  "mode": "all_matches",
  "rules": [
    {
      "id": "high-value",
      "priority": 100,
      "when": {"path": "$.total", "op": "gte", "value": 1000},
      "then": [{"emit": {"topic": "orders.review", "data": {"id": "$.id"}}}]
    },
    {
      "id": "us-order",
      "priority": 50,
      "when": {"path": "$.country", "op": "eq", "value": "US"},
      "then": [{"emit": {"topic": "orders.us", "data": {"id": "$.id"}}}]
    }
  ]
}
```

### Result

```json
{
  "matched_rules": ["high-value", "us-order"],
  "effects": [
    {
      "id": "eff-abc123",
      "destination": "orders.review",
      "name": "orders.review",
      "payload": {"id": "order-4821"},
      "effect_type": "emit"
    },
    {
      "id": "eff-def456",
      "destination": "orders.us",
      "name": "orders.us",
      "payload": {"id": "order-4821"},
      "effect_type": "emit"
    }
  ],
  "hash": "7a8b9c0d1e2f..."
}
```

---

## Limits

| Limit | Default | Description |
|---|---|---|
| Max rule set size | 64KB | Source JSON size |
| Max rules per set | 100 | Number of rules |
| Max predicates per rule | 200 | Total leaf predicates |
| Max nesting depth | 10 | Predicate tree depth |
| Max actions per rule | 10 | Then + Otherwise |
| Max payload size | 256KB | Action data size |
| Max rule ID length | 128 | Characters |