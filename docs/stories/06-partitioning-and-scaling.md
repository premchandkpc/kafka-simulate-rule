# Story 06 — Partitioning and scaling

## Ordering contract

Choose `partition_key` as the smallest business entity whose transitions must be ordered: `order-123`, `account-88`, or `device-7`. Do not use a tenant ID unless all tenant activity must be serialized.

The domain helper calculates the intended virtual shard as:

```text
virtual_shard = FNV-1a(tenant_id + ":" + partition_key) mod shard_count
```

The helper exists, as does a SQL shard-lease repository with fencing tokens. The running worker does **not** currently call either one. Its single durable JetStream consumer fetches and processes one delivery at a time, so ordering and scale are broker/instance behavior rather than the documented keyed-partition guarantee.

## Target implementation

```text
event -> calculate stable virtual shard -> worker acquires shard lease (fencing token)
      -> per-shard serial queue -> transactional ProcessEvent -> broker ACK
                                            |
                              different virtual shards run concurrently
```

Use many virtual shards (for example, 4,096), but assign only those shards to live worker replicas. That gives fine-grained rebalancing without changing producer keys. Store routing epoch and fencing token with all state writes that require exclusive ownership; reject writes from an expired owner.

## Consumer design

Use durable pull consumers and fetch only the capacity that the worker/database can absorb. A safe initial shape is:

```text
fetch batch <= available worker slots
route each delivery to its shard queue
one active execution per shard
bounded total concurrent transactions
stop fetching during drain; ACK only committed work
```

Do not ACK on receipt. Set `AckWait` longer than worst-case bounded transaction time, renew/extend processing when the transport supports it, and use a finite `MaxDeliver` plus durable quarantine. Preserve broker metadata for diagnostics.

## Scaling dimensions

| Component | Scale signal | Guardrail |
| --- | --- | --- |
| API | request rate/latency | DB pool and rate limits |
| Worker | oldest message age, backlog, CPU, transaction latency | shard ownership, DB connections, per-tenant limits |
| Publisher | pending outbox age, destination latency | destination-specific concurrency/circuit breakers |
| PostgreSQL | CPU, IOPS, locks, connections | short transactions, indexes, replicas/backups |
| NATS | stream bytes, consumer lag, replication health | retention, replicas, storage headroom |

Scale workers only until database contention or a hot shard becomes the limiting factor. More workers cannot accelerate one partition key. For hot keys, consider a domain change that permits subkeys; do not silently change the key because it breaks ordering.

## Rebalancing and deployment

On a worker rollout: mark instance draining, stop new fetches, finish bounded in-flight executions, release/expire leases, then terminate. A replacement acquires the shard with a higher fencing token and resumes safely through inbox deduplication.

Never change a modulo shard count in place: it remaps almost every key. Keep a fixed virtual-shard count, or introduce a versioned routing epoch and dual-read/controlled migration. Validate behavior with tests for same-key serialization, cross-key concurrency, worker crash before/after commit, lease expiry, duplicate delivery, and rebalance.
