# Data Model and Consistency

## Transaction boundary

The database transaction covers only local durable facts:

```text
inbox insert -> active revision read/pin -> evaluate -> execution audit
           -> owned state mutation -> outbox insert -> inbox committed
```

The broker acknowledgement and destination call happen outside the transaction. This is a deliberate outbox pattern, not distributed exactly-once.

## Tables (implemented in `migrations/`)

| Table | Key | Important columns | Invariant | Status |
| --- | --- | --- | --- | --- |
| `rule_revisions` | `(tenant_id, rule_set, revision)` | source, compiled artifact, hash, compiler version | immutable after publish | DONE |
| `rule_activations` | `(tenant_id, rule_set)` | revision, activation version, actor | one visible revision | DONE |
| `inbox` | `(tenant_id, event_id)` | status, first_seen_at, committed_at | duplicate identity is unique | DONE |
| `executions` | `execution_id` | event ID, revision, decision hash, status | revision never changes | DONE |
| `outbox_effects` | `effect_id` | execution ID, destination, payload ref, status, available_at | effect ID is unique | DONE |
| `quarantine` | `quarantine_id` | source ID, error class, payload ref, replay state | replay is authorized and audited | DONE |
| `shard_leases` | `virtual_shard` | owner, fencing token, expires_at | stale owners cannot commit | DONE |

Use foreign keys where retention and partitioning allow. Use check constraints for status values. Partition high-volume execution, inbox, outbox-attempt, and audit tables by tenant/time only after measured volume justifies it; do not prematurely partition every table.

## Idempotency and crash cases

1. Before commit: rollback; broker redelivers.
2. After commit before ack: redelivery sees the inbox key and returns the recorded result.
3. Publisher sends effect and crashes before status update: it sends the same `effect_id` again; the destination must deduplicate.
4. Non-idempotent destination: mark the effect as requiring reconciliation and expose it operationally. Do not hide this behind a retry loop.

Unique constraints are the final defense. Caches, locks, and in-memory deduplication are not correctness mechanisms.

## Isolation and locking

Start with `READ COMMITTED`. Use a unique inbox insert for duplicate detection and row locks only for mutable aggregate state. Use serializable isolation only where a measured aggregate invariant requires it. Keep transactions short, avoid network I/O, and test deadlocks and retry behavior.

Shard lease writes use a fencing token. Every worker-owned mutation includes the current token or is protected by a lease condition, so an expired worker cannot commit after ownership changes.

## Retention and payload size

Keep indexed metadata and hashes in SQL. Store large event/effect payloads in immutable object storage and retain a content hash plus URI in SQL. Retention must be defined independently for source events, execution audit, outbox attempts, and quarantine. Replay is impossible after source payload deletion unless the operator intentionally accepts a reduced audit record.

## Consistency claims

| Boundary | Guarantee |
| --- | --- |
| SQL transaction | ACID for local records |
| Broker to SQL | At least once with idempotent inbox |
| SQL to destination | At least once unless destination deduplicates |
| Rule activation | Atomic pointer change for new executions |
| Retry | Original execution revision remains pinned |
| Read API | Eventually consistent for projections; direct execution lookup is authoritative |

Never advertise global exactly-once or global ordering.