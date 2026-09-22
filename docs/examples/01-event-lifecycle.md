# Example 1 — Complete event lifecycle

## The event

Producer sends:

```json
{
  "id": "evt-1001",
  "type": "order.created",
  "tenant_id": "tenant-a",
  "partition_key": "order-5001",
  "occurred_at": "2026-09-22T10:00:00Z",
  "data": {
    "order_id": "5001",
    "amount": 15000,
    "country": "IN",
    "customer_tier": "GOLD"
  }
}
```

## Step-by-step trace

```text
Producer
   |
   | order.created
   v
NATS JetStream
   |
   v
Durable Consumer
   |
   v
Worker.Fetch(10)
   |
   +-- delivery.Event() -> EventEnvelope
   |
   v
ProcessEventUseCase.Execute(envelope)
   |
   +-- envelope.Validate()
   |     checks: id, type, tenant_id, partition_key, occurred_at
   |
   +-- BEGIN TRANSACTION
   |
   +-- Inbox.Get(tenant_id="tenant-a", event_id="evt-1001")
   |     -> nil (first time)
   |
   +-- Inbox.Insert(tenant_id="tenant-a", event_id="evt-1001", status="processing")
   |     -> true (inserted)
   |
   +-- Activation.Get(tenant_scope="tenant-a", rule_set="order.created")
   |     -> { revision: 7, version: 3 }
   |
   +-- RuleRepo.GetActive(tenant_scope="tenant-a", rule_set="order.created")
   |     -> RuleRevision { rule_id: "order.created", revision: 7, match_mode: "first_match" }
   |
   +-- Evaluator.Evaluate(revision, event, facts=nil)
   |     -> Decision { matched: ["high-value-order"], effects: [...] }
   |
   +-- Executions.Save(execution)
   |     execution_id = UUID
   |     revision = 7
   |     decision_hash = SHA-256(...)
   |
   +-- Outbox.Insert(effects)
   |     effect_id = SHA-256(tenant|event|rule_set|revision|rule_id|action_index)
   |     destination = "fraud-service"
   |     status = "pending"
   |
   +-- Inbox.MarkCommitted(tenant_id="tenant-a", event_id="evt-1001", execution_id=...)
   |
   +-- COMMIT
   |
   +-- delivery.Ack()
   |
   v
Outbox Publisher (separate goroutine, 5s ticker)
   |
   +-- Outbox.ClaimPending(batchSize=10, owner="publisher")
   |     -> UPDATE SET status='claimed', claimed_by='publisher'
   |
   +-- EffectSender.Send(effect)
   |     -> HTTP POST to fraud-service
   |
   +-- Outbox.MarkDelivered(effect_id)
   |
   v
Done
```

## What each table looks like after processing

### inbox

| tenant_id | event_id | status | execution_id | first_seen_at | committed_at |
|-----------|----------|--------|--------------|---------------|--------------|
| tenant-a | evt-1001 | committed | exec-uuid | 10:00:01 | 10:00:01 |

### executions

| execution_id | event_id | tenant_id | rule_set | revision | decision_hash | status |
|-------------|----------|-----------|----------|----------|---------------|--------|
| exec-uuid | evt-1001 | tenant-a | order.created | 7 | abc123... | completed |

### outbox_effects

| effect_id | execution_id | destination | name | status |
|-----------|-------------|-------------|------|--------|
| SHA-256(...) | exec-uuid | fraud-service | review-order | delivered |

### rule_revisions

| tenant_scope | rule_id | revision | match_mode |
|-------------|---------|----------|------------|
| tenant-a | order.created | 7 | first_match |

### rule_activations

| tenant_scope | rule_set | revision | version |
|-------------|----------|----------|---------|
| tenant-a | order.created | 7 | 3 |

## Current implementation status

| Step | Status |
|------|--------|
| NATS consume | ✅ implemented |
| Validate envelope | ✅ implemented |
| BEGIN transaction | ✅ implemented |
| Inbox dedup | ✅ implemented |
| Read activation | ✅ implemented |
| Read rule revision | ✅ implemented |
| Pure evaluation | ✅ implemented |
| Insert execution | ✅ implemented |
| Insert outbox | ✅ implemented |
| Mark inbox committed | ✅ implemented |
| COMMIT | ✅ implemented |
| Broker ACK | ✅ implemented |
| Outbox publisher | ✅ implemented (ticker-based) |
| Effect delivery | ⚠️ FakeDestination (not real HTTP) |
