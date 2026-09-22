# Example 5 — Rule revisions and activation

## Immutable revisions

Each rule set has numbered revisions. Once published, a revision never changes.

```text
order.created
   |
   +-- revision 1 (original)
   +-- revision 2 (added gold-customer rule)
   +-- revision 3 (tuned thresholds)
```

## Activation pointer

`rule_activations` holds one active revision per `(tenant_scope, rule_set)`:

```text
tenant-a, order.created -> revision 3
```

## Event pins the active revision

```text
evt-1001 arrives
   |
   v
Read activation -> revision 3
   |
   v
Evaluate evt-1001 with revision 3
   |
   v
Save execution with revision=3
```

Now revision 4 is activated:

```text
Activate revision 4
```

New event:

```text
evt-1002 arrives
   |
   v
Read activation -> revision 4
   |
   v
Evaluate evt-1002 with revision 4
```

But evt-1001 still used revision 3. Its execution record says `revision=3`.

```text
evt-1001 -> revision 3 (pinned)
evt-1002 -> revision 4 (pinned)
```

## Retry uses the pinned revision

Worker crashes while processing evt-1001. Broker redelivers.

```text
evt-1001 redelivered
   |
   v
Inbox.Get -> entry exists (status=processing)
   |
   v
Execution.Get -> revision=3
   |
   v
Continue with revision 3 (NOT revision 4)
```

Even though revision 4 is now active, the retry uses the originally recorded revision.

## Activate a new revision via API

```bash
curl -X POST http://localhost:8080/api/v1/rules/order.created/activate \
  -H 'Content-Type: application/json' \
  -d '{
    "revision": 4,
    "source": {
      "rule_set": "order.created",
      "revision": 4,
      "mode": "first_match",
      "rules": [
        {
          "id": "high-value",
          "priority": 100,
          "when": {
            "all": [
              { "path": "$.data.amount", "op": "gte", "value": 10000 },
              { "path": "$.data.customer_tier", "op": "eq", "value": "GOLD" }
            ]
          },
          "then": [
            { "emit": { "topic": "orders.review", "data": {} } }
          ]
        },
        {
          "id": "normal",
          "priority": 10,
          "when": { "path": "$.data.order_id", "op": "exists" },
          "then": [
            { "emit": { "topic": "orders.process", "data": {} } }
          ]
        }
      ]
    }
  }'
```

This:

1. Compiles the source JSON
2. Saves the revision to `rule_revisions` (immutable)
3. Updates `rule_activations` to point to the new revision

## Revision audit trail

Every execution records which revision it used:

```text
execution_id | event_id | revision | decision_hash
-------------|----------|----------|---------------
exec-001     | evt-1001 | 3        | abc123...
exec-002     | evt-1002 | 4        | def456...
```

You can trace:

```text
What event?     -> evt-1001
Which revision? -> 3
What decision?  -> decision_hash=abc123
What effects?   -> outbox_effects linked to execution
```

## Current implementation status

| Feature | Status |
|---------|--------|
| Immutable revisions | ✅ schema + code |
| Activation pointer | ✅ schema + code |
| Revision pinning in execution | ✅ code |
| Retry uses pinned revision | ✅ code |
| API to activate revision | ✅ implemented |
| Event contract version linkage | ⚠️ not yet tracked |
