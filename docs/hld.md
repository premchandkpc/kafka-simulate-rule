# High-level design

## Scope

FlowRule evaluates versioned business rules against durable events and reliably creates follow-up effects. It is multi-tenant, horizontally scalable, replayable, and operationally simple.

| Attribute | Architectural response |
| --- | --- |
| Correctness | Immutable revisions, transactional inbox/outbox, audit trail |
| Scale | Keyed partitions, stateless workers, bounded pull concurrency |
| Availability | Broker redelivery, SQL HA, worker lease expiry, graceful drain |
| Latency | Compile rules at activation and cache revisions |
| Security | Tenant isolation, RBAC, allowlisted effects, authenticated ingress |
| Operability | Execution state, lag/age/error SLOs, replay and quarantine |

Targets must be selected per deployment. Initial SLOs should separate durable acceptance, rule-evaluation latency, and external effect latency.

## Context

```text
clients -> Ingress API -> durable broker -> worker pool -> SQL inbox/execution/outbox -> effect publisher -> destinations
                         ^                   |                    ^
                         |                   v                    |
                         +------------- Rule Registry -------------+
                                     Admin / Query API
```

The engine does not own service discovery, distributed consensus, or broker storage. Those are platform concerns.

## Bounded contexts

| Context | Aggregate | Invariants |
| --- | --- | --- |
| Rule management | `RuleSet`, `RuleRevision`, `Activation` | Revision is immutable; one active policy per tenant scope |
| Event intake | `EventEnvelope`, `InboxEntry` | Event ID unique per tenant; envelope valid before acceptance |
| Decisioning | `Decision`, `Effect` | Evaluation is pure and deterministic |
| Effect delivery | `OutboxEffect`, `DeliveryAttempt` | Effect ID unique; delivery state only moves forward |
| Operations | `QuarantineEntry`, `ReplayRequest` | Replay is authorized and auditable |

The evaluator receives immutable `RuleRevision`, `EventEnvelope`, and optional fact snapshot. It cannot access repositories or network clients.

## Partitioning and tenancy

Choose `partition_key` from the business entity whose transitions must be ordered: order, account, or device. Do not use tenant alone unless tenant-wide serial execution is intended.

```text
virtual_shard = hash(tenant_id + ':' + partition_key) mod 4096
```

- A worker owns one virtual shard at a time through a lease or broker consumer assignment.
- Events for a shard run serially; different shards run concurrently.
- Store `routing_epoch` with the execution. Partition-count changes require a migration plan because modulo changes remap keys.
- Apply tenant quotas for ingress, active keys, rule size, effects/event, and retention to prevent noisy neighbors.

## Consistency model

ACID applies inside SQL, not across SQL, the broker, and HTTP. Avoid two-phase commit.

```text
broker delivery -> SQL transaction (inbox + pinned revision + execution + outbox) -> commit -> broker ack -> idempotent effect delivery
```

This provides atomic local transitions, at-least-once broker consumption, and effectively-once effects only when a destination honors the idempotency key. Read models are eventually consistent. Do not claim global exactly-once.

## Rollout and failure model

Revisions are immutable and activation is a single audited registry transaction. New events pin the visible revision. Retries use the initially recorded revision. Canary selection hashes the partition key; rollback changes the pointer for new events only.

| Failure | Response |
| --- | --- |
| Before SQL commit | Broker redelivers; no durable execution |
| After commit, before ack | Redelivery hits inbox duplicate defense |
| Effect delivered, status missing | Republish same effect ID; destination deduplicates |
| Worker loss | Lease/consumer transfers; transaction rolls back or replay deduplicates |
| Poison event | Bounded retries then quarantine; unrelated shards continue |

## Deployment

Deploy API, workers, and effect publishers independently. Scale API by request rate, workers by backlog/database headroom, and publishers by destination capacity. Production requires HA SQL with backups/PITR, replicated broker storage, mTLS/TLS, non-root resource-limited containers, disruption budgets, and readiness that excludes draining instances.
