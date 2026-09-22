# Example 7 — Interview-level scenarios

## "Why NATS JetStream?"

```text
Kafka:
  + better ecosystem
  + more operators know it
  - JVM overhead
  - partition rebalancing complexity
  - heavier operational footprint

NATS:
  + single binary
  + JetStream gives durability
  + subject-based routing
  + lower latency
  - smaller ecosystem
  - fewer operators
```

FlowRule uses NATS because:

1. Subject-based routing maps naturally to shard routing
2. Single binary is easier to deploy than a Kafka cluster
3. JetStream provides at-least-once delivery with durable consumers
4. Lower operational overhead for a system that isn't at Kafka scale yet

The project explicitly lists Kafka as Phase 4 (managed queue alternative).

## "Why Postgres?"

```text
Postgres:
  + ACID transactions
  + JSONB for flexible data
  + row-level locking (FOR UPDATE SKIP LOCKED)
  + mature, well-understood
  + won't lose data

Redis:
  + fast
  - not durable by default
  - no complex transactions

MongoDB:
  + flexible schema
  - weaker consistency guarantees
  - no multi-document ACID in the same way
```

FlowRule needs:

1. **Transactional inbox/outbox**:必须在同一事务中保证原子性
2. **Row-level locking**: `FOR UPDATE SKIP LOCKED` for outbox claiming
3. **Durability**: event processing results must survive crashes
4. **JSONB**: for storing compiled rules and event data

Postgres is the natural choice for a system that values correctness over raw speed.

## "Why transactional outbox?"

Direct publish:

```text
Process event
   |
   v
Send to fraud-service  <-- if this fails, what happens?
   |
   v
Save to database       <-- if this succeeds but send failed, inconsistent
```

Transactional outbox:

```text
BEGIN
   process event
   INSERT into outbox
COMMIT
   |
   v
Publisher reads outbox
   |
   v
Send to fraud-service
   |
   v
Mark delivered
```

Benefits:

1. **Atomicity**: event processing and effect creation are in one transaction
2. **No dual-write**: we don't write to DB and publish to NATS in the same transaction
3. **Retry-safe**: outbox publisher can retry failed deliveries
4. **Idempotent**: deterministic effect IDs prevent duplicate side effects

## "Why not exactly-once?"

```text
FlowRule -> PaymentService

FlowRule sends: "charge $15,000"
PaymentService charges successfully
PaymentService sends response
   |
   X  network dies
   |
FlowRule sees: TIMEOUT
```

FlowRule cannot know:

```text
A) Payment never happened
B) Payment happened but response was lost
```

If it retries blindly:

```text
$15,000 + $15,000 = $30,000  (double charge)
```

The solution is **effect_id as idempotency key**:

```text
effect_id = SHA-256(tenant|event|rule_set|revision|rule_id|action_index)

First request:  effect_id=abc123 -> charge $15,000
Retry:          effect_id=abc123 -> recognize duplicate, return previous result
```

Exactly-once is only possible when the destination honors the idempotency key.

## "Why not Kafka?"

Kafka is a valid choice. FlowRule's architecture is queue-agnostic:

```text
Phase 1: NATS JetStream (current)
Phase 4: Kafka / managed queue (target)
```

The outbox pattern works with any durable queue. The key properties are:

1. At-least-once delivery
2. Consumer groups for parallel processing
3. Message ordering within a partition

Kafka provides all of these. The choice of NATS was pragmatic (simpler deployment, subject-based routing), not architectural.

## "Why partition by business key?"

```text
Partition by tenant:
  - all events for tenant-a are ordered
  - but tenant-a has 50,000 events/sec
  - hot key problem

Partition by order-5001:
  - only order-5001 events are ordered
  - order-5002 can run in parallel
  - finer granularity
```

The principle: choose the **smallest entity whose transitions must be ordered**.

If order-5001's CREATED must come before PAID, partition by order-5001.
If all tenant-a events must be ordered, partition by tenant-a (but accept the hot key cost).

## "What happens if two workers process the same event?"

```text
Worker A                        Worker B
   |                               |
   v                               v
BEGIN                           BEGIN
   |                               |
   v                               v
Inbox.Get -> nil                 Inbox.Get -> nil
   |                               |
   v                               v
Inbox.Insert -> true             Inbox.Insert -> false (UNIQUE constraint)
   |                               |
   v                               v
Evaluate, execute, outbox        Inbox.Get -> existing entry
   |                               |
   v                               v
COMMIT                           return existing execution
   |                               |
   v                               v
ACK                              ACK
```

Both workers complete. One execution is created. The UNIQUE constraint on `(tenant_id, event_id)` is the defense.

## "How does FlowRule scale?"

```text
Current (Phase 1):
  - single worker
  - shared NATS consumer
  - no shard ownership

Target (Phase 2):
  - N workers
  - each owns subset of shards
  - shard leases for ownership
  - fencing tokens prevent split-brain

Scaling dimensions:
  1. More workers -> more shards -> more throughput
  2. Read scaling: rule revisions are read-only, can be cached
  3. Write scaling: outbox claiming is parallel (FOR UPDATE SKIP LOCKED)
  4. Effect scaling: outbox publisher is independent of event processing
```

The bottleneck is the single Postgres writer. At scale, you'd shard the database or move to a distributed SQL system.

## "What's the mental model?"

Seven objects:

```text
Event -> Rule Revision -> Decision -> Execution -> Effect -> Outbox -> Destination
```

Five correctness mechanisms:

```text
1. Immutable revision (pin the rule)
2. Deterministic evaluator (pure function)
3. Inbox deduplication (idempotent processing)
4. Transactional outbox (atomic side effects)
5. Idempotent effect delivery (effect_id as key)
```

Everything else exists to make those invariants scale.
