# Example 4 — Partitioning and ordering

## Business key as the ordering unit

```text
partition_key = order-5001
```

FlowRule computes:

```text
virtual_shard = hash("tenant-a:order-5001") % 4096
```

Maybe:

```text
virtual_shard = 57
```

All events with `partition_key = order-5001` go to shard 57.

## Same-key ordering

```text
order-5001 -> CREATED    -> shard 57
order-5001 -> PAID       -> shard 57
order-5001 -> SHIPPED    -> shard 57
```

Processing order is guaranteed:

```text
CREATED -> PAID -> SHIPPED
```

## Cross-key parallelism

```text
order-5001 -> shard 57   -> serialized
order-5002 -> shard 203  -> serialized
order-5003 -> shard 57   -> serialized (same shard as 5001)
order-5004 -> shard 812  -> serialized
```

```text
shard 57:   order-5001 -> order-5003 (serialized)
shard 203:  order-5002 (independent)
shard 812:  order-5004 (independent)
```

Shards 57, 203, and 812 run concurrently. Within shard 57, events are serialized.

## Worker ownership

Target: 4096 virtual shards, N workers.

```text
Worker A: shards 0-1023
Worker B: shards 1024-2047
Worker C: shards 2048-3071
Worker D: shards 3072-4095
```

Each worker only processes events in its assigned shards.

## Hot key problem

```text
customer-999 generates 50,000 events/sec
```

All events have:

```text
partition_key = customer-999
```

They all hash to the same shard:

```text
customer-999 -> shard 57 (always)
```

Result:

```text
shard 57: 50,000 events/sec (bottleneck)
shard 203: 100 events/sec (idle)
shard 812: 50 events/sec (idle)
```

The system does not pretend this doesn't happen. Hot keys should be visible through metrics.

Mitigation: choose `partition_key` as the smallest entity that needs ordering. If customer-level ordering isn't required, use `order-5001` instead of `customer-999`.

## Fencing token

When a worker takes ownership of a shard, it gets a fencing token:

```text
Worker A acquires shard 57
  -> fencing_token = 1
  -> expires_at = NOW() + 30s
```

If Worker A becomes slow and the lease expires:

```text
Worker B acquires shard 57
  -> fencing_token = 2
  -> expires_at = NOW() + 30s
```

Now Worker A tries to write:

```text
Worker A writes with fencing_token=1
  -> REJECTED (stale token)
```

Worker B writes with fencing_token=2:

```text
Worker B writes with fencing_token=2
  -> ACCEPTED
```

This prevents split-brain processing.

## Current implementation status

| Feature | Status |
|---------|--------|
| VirtualShard computation | ✅ implemented (types.go:33-37) |
| ShardLease table | ✅ schema exists (migration 006) |
| ShardLeaseRepository | ✅ implemented (sql/shard_lease.go) |
| Worker shard ownership | ⚠️ not yet enforced |
| Fencing token writes | ⚠️ not yet enforced |
| Subject-per-shard routing | ⚠️ not yet implemented |
| Single shared consumer | ✅ current behavior |
