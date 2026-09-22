# Target architecture

## One execution path

```text
producer
  -> JetStream stream (or Kafka topic)
  -> durable pull consumer
  -> partition worker: validate -> select revision -> evaluate -> stage effects
  -> transactional store
       ├─ inbox: accepted event IDs
       ├─ rule revisions + activation pointer
       ├─ execution result / audit record
       └─ outbox: commands and emitted events
  -> outbox publisher -> external services / output subjects
```

One worker owns a `(tenant, rule-set, partition)` at a time. Events with one `partition_key` land on the same partition, so state changes for that key are ordered. Different keys execute concurrently. This is the scaling and correctness unit—not a cluster-wide scheduler lane.

## Boundaries (implemented)

| Component | Responsibility | Does not do | Status |
| --- | --- | --- | --- |
| Ingress | Validate envelope and publish durably | Evaluate rules | DONE |
| Broker adapter | Fetch, ack/nak, retry, publish | Define business semantics | DONE |
| Rule registry | Immutable revisions and atomic activation pointer | Broadcast plans | DONE |
| Partition worker | Evaluate and atomically write inbox/audit/outbox | Coordinate all workers | DONE |
| Effects publisher | Deliver outbox records with retries | Mutate rule state | DONE |
| Query API | Deploy, activate, replay, inspect | Participate in delivery | DONE |

Use Postgres (or equivalent transactional SQL) for the registry, inbox, execution audit, outbox, and partition leases. Use Kubernetes deployment plus leases/consumer ownership for coordination. This removes custom Raft, gossip, file recovery, plan acknowledgements, and lane scheduling.

## Transport choice

| Option | Use when | Trade-off | Status |
| --- | --- | --- | --- |
| NATS JetStream | Low latency, request/reply, durable work queues, simple operations | Smaller analytics/retention ecosystem | DONE |
| Kafka adapter | Kafka is already the event backbone or long retention is central | Higher operational and client complexity | TODO |
| Cloud queue adapter | Managed operations matter more than portability | Provider-specific semantics | TODO |

Expose only `Fetch`, `Ack`, `Retry`, `Publish`, and message metadata to the engine. Do not expose Kafka partitions, consumer groups, or JetStream subjects to rule evaluation; mappings belong in deployment configuration.

## Delivery semantics

At-least-once is the base guarantee. Get effective-once business behavior with a stable event ID plus transactional inbox/outbox:

1. Insert or lock `(tenant, event_id)` in `inbox`.
2. On conflict, acknowledge the duplicate without evaluating actions.
3. Evaluate the pinned rule revision and commit audit/state/outbox in one transaction.
4. Acknowledge the broker only after commit.
5. Retry each outbox effect until its destination accepts its idempotency key.

An arbitrary HTTP call cannot be exactly-once. Send `idempotency_key = execution_id + action_index`; a destination must deduplicate it. Otherwise model the effect as an explicit reconcilable operation.

## Revision rollout

Revisions are immutable. `rule_activation` maps a rule set and tenant scope to one revision in a single audited transaction.

- New events pin the visible revision when execution starts.
- Retries use the revision recorded by the initial execution.
- Canary rollout hashes `partition_key`; it never randomly switches per retry.
- Rollback moves the activation pointer for new executions and never rewrites history.

Workers cache `(rule_id, revision)` and invalidate through a monotonic registry version or notification. A cache miss reads the source of truth, so plan distribution is not on the critical path.

## Backpressure and recovery

- Pull only work local concurrency and database capacity can handle.
- Persist retry attempt and due time; use exponential backoff with jitter.
- Quarantine exhausted/non-retryable messages with event, revision, error class, and trace ID.
- Apply destination limits and circuit breakers in the effects publisher, not the pure evaluator.
- On worker loss, its lease expires and another worker resumes broker delivery. Inbox/outbox makes redelivery safe.

## Code shape (implemented)

```text
cmd/api                  deploy, activate, inspect, replay               DONE
cmd/worker               fetch loop and graceful shutdown                 DONE
internal/domain          Rule, Event, Decision, Effect, Errors            DONE
internal/rules           parse, validate, compile index, evaluate         DONE
internal/ports           interfaces for broker, store, clock, effects    DONE
internal/adapters/sql    SQL registry + inbox/outbox + leases             DONE
internal/adapters/nats   JetStream consumer and publisher                 DONE
internal/adapters/effects outbox publishing and destination adapters      DONE
internal/application     use cases: ProcessEvent, PublishEffects, Activate DONE
```

Keep evaluation pure: `Decision Evaluate(Rule, Event, Facts)`. It needs no broker, database, goroutine, CGo, or FFI dependency.