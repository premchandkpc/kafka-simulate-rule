# Partitions and scaling story

The engine is built to scale by entity ordering, not by global total ordering.

## 1. Partitioning principle

The main architectural rule is: one business key has a single ordered stream. Different keys can be processed independently.

The project uses a virtual shard derived from the event’s tenant and partition key:

```text
virtual_shard = hash(tenant_id + ':' + partition_key) mod 4096
```

This value is expressed in the domain model with `VirtualShard` and the `VirtualShard()` method in [internal/domain/types.go](../../internal/domain/types.go).

## 2. Why partitioning matters

If all events were processed globally in a single queue, the system would become a single bottleneck. Instead, FlowRule turns each event into a routed unit of work for a shard.

This means:

- account-level changes are ordered
- order-level changes are ordered
- unrelated business entities can run in parallel
- a hot key does not block unrelated traffic

## 3. Shard ownership

The `ShardLease` model tracks ownership and leases, making it possible for workers to claim ownership of a shard for a bounded time.

A worker can own one or more shards at the same time. Lease expiry and fencing tokens allow safe handoff when a worker fails or drains.

The design is documented in the deeper docs:

- [docs/hld.md](../hld.md)
- [docs/lld.md](../lld.md)

## 4. Scaling model

### Horizontal scaling

Add more worker instances to increase the number of shards that can be processed concurrently.

### Backpressure

The worker should only fetch a bounded batch that the database and downstream system can support.

### Operational scaling

The API, worker, and publisher are each scaled according to their own bottleneck:

- API: request rate and validation cost
- worker: backlog and rule evaluation throughput
- publisher: outbox send rate and destination capacity

## 5. What is intentionally not distributed

FlowRule avoids a heavyweight coordination layer such as custom consensus or service registry logic. The engine does not require a complex scheduler or cluster-wide authority system.

Instead, it uses:

- a durable broker
- a transactional SQL store
- lease-based ownership
- deterministic routing by business key

That makes the system easier to operate and easier to reason about.

## 6. Hot key behavior

A hot key is a single partition key with large event volume. It remains ordered by design. That is intentional because the system protects business correctness over global throughput.

The trade-off is:

- one hot key can become a bottleneck
- but ordering remains correct
- and the rest of the system keeps running

## 7. Partitioning summary

The scaling story is:

```text
tenant + partition_key -> virtual shard -> worker ownership -> ordered event stream -> durable execution
```

This is the core design principle behind the whole engine.
