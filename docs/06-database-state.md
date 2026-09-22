# Example 6 — Database state walkthrough

## Schema overview

```text
rule_revisions      (tenant_scope, rule_id, revision)   PK
rule_activations    (tenant_scope, rule_set)            PK
inbox               (tenant_id, event_id)               PK
executions          (execution_id)                      PK
outbox_effects      (effect_id)                         PK
shard_leases        (virtual_shard)                     PK
quarantine          (quarantine_id)                     PK
```

## Complete state after one event

Starting state: empty database.

Event: `evt-1001`, `order.created`, `tenant-a`, `order-5001`, amount=15000.

### 1. Rule published and activated

```sql
INSERT INTO rule_revisions (tenant_scope, rule_id, revision, source, compiled, content_hash, compiler_version, match_mode)
VALUES ('tenant-a', 'order.created', 1, '...', '...', '...', '1.0.0', 'first_match');

INSERT INTO rule_activations (tenant_scope, rule_set, revision, version, actor)
VALUES ('tenant-a', 'order.created', 1, 1, 'admin');
```

### 2. Event processed

```sql
-- Inbox
INSERT INTO inbox (tenant_id, event_id, status, first_seen_at, committed_at, execution_id)
VALUES ('tenant-a', 'evt-1001', 'committed', NOW(), NOW(), 'exec-001');

-- Execution
INSERT INTO executions (execution_id, event_id, tenant_id, rule_set, revision, decision_hash, status, created_at, completed_at)
VALUES ('exec-001', 'evt-1001', 'tenant-a', 'order.created', 1, 'abc123...', 'completed', NOW(), NOW());

-- Outbox
INSERT INTO outbox_effects (effect_id, execution_id, destination, name, payload, effect_type, status, attempts, max_attempts, available_at, created_at, updated_at)
VALUES ('effect-001', 'exec-001', 'fraud-service', 'review-order', '...', 'emit', 'pending', 0, 5, NOW(), NOW(), NOW());
```

### 3. Effect delivered

```sql
UPDATE outbox_effects
SET status = 'delivered', claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL, updated_at = NOW()
WHERE effect_id = 'effect-001';
```

## Query patterns

### Find all executions for an event

```sql
SELECT * FROM executions WHERE event_id = 'evt-1001';
```

### Find what revision was used

```sql
SELECT e.execution_id, e.revision, e.decision_hash, e.status
FROM executions e
WHERE e.event_id = 'evt-1001';
```

### Find pending effects

```sql
SELECT * FROM outbox_effects
WHERE status = 'pending' AND available_at <= NOW()
ORDER BY available_at, created_at
LIMIT 10;
```

### Find quarantined items

```sql
SELECT * FROM quarantine
WHERE source_type = 'effect'
ORDER BY created_at DESC;
```

### Check shard ownership

```sql
SELECT virtual_shard, owner, fencing_token, expires_at
FROM shard_leases
WHERE expires_at > NOW();
```

### Count events per tenant today

```sql
SELECT tenant_id, COUNT(*)
FROM executions
WHERE created_at >= CURRENT_DATE
GROUP BY tenant_id;
```

## Outbox state machine

```text
pending
   |
   v  ClaimPending (UPDATE SET status='claimed')
claimed
   |
   +---> delivered (MarkDelivered)
   |
   +---> pending (ScheduleRetry, status='pending', available_at=future)
   |
   +---> quarantined (Quarantine, max attempts reached)
```

## Inbox state machine

```text
processing
   |
   v  MarkCommitted (UPDATE SET status='committed', execution_id=...)
committed
```

The inbox never goes back to `processing`. A `processing` entry with no matching `committed` execution indicates a crash before commit.

## Current implementation status

| Table | Schema | Repository | In transaction |
|-------|--------|------------|----------------|
| rule_revisions | ✅ | ✅ | ✅ (read) |
| rule_activations | ✅ | ✅ | ✅ (read) |
| inbox | ✅ | ✅ | ✅ (insert, mark) |
| executions | ✅ | ✅ | ✅ (insert, update) |
| outbox_effects | ✅ | ✅ | ✅ (insert) |
| shard_leases | ✅ | ✅ | ⚠️ not in event tx |
| quarantine | ✅ | ✅ | ⚠️ not in event tx |
