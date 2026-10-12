# Delivery Reliability Contract

## Status

This document defines the target behavior for FlowRule's broker-backed runtime. It is a contract and implementation checklist, **not a claim that every item is implemented**. The current runtime uses NATS JetStream and PostgreSQL. PostgreSQL is authoritative for business processing; JetStream is the delivery mechanism.

## Architecture decision

Do not implement a Kafka clone or a new broker protocol. Keep NATS JetStream as the first adapter and make the application depend on transport-neutral ports. Add a Kafka adapter only when measured workload or deployment requirements justify it.

The reliability layer is the product: durable processing state, bounded retry, per-key ordering, quarantine, replay, effect delivery, recovery, and operational visibility. A broker ACK cannot mean that an external side effect completed.

## Three separate outcomes

1. **Transport delivery**: a broker made an event available to a consumer.
2. **Business processing**: PostgreSQL committed the inbox identity, execution decision, and outbox effects atomically.
3. **Effect delivery**: an external destination accepted an outbox effect, with destination-side idempotency or reconciliation for ambiguous outcomes.

These states must be represented and observed separately.

## Required settlement rules

| Situation | Required action | Reason |
|---|---|---|
| Event transaction committed | ACK the broker delivery | Durable processing is complete |
| Duplicate delivery with an already committed inbox/execution | ACK after verifying the committed result | Redelivery is expected after commit-before-ACK crashes |
| Database/transaction failure before durable handoff | Do not ACK; request retry or allow visibility timeout/redelivery | The event has not been safely handed off |
| Retryable failure durably scheduled in PostgreSQL | ACK original delivery only after schedule commit | The database now owns retry responsibility |
| Permanent invalid event durably quarantined | ACK original delivery only after quarantine commit | Poison payload is recoverable and does not loop forever |
| Quarantine or retry persistence fails | Do not ACK; retry delivery | Never discard the only durable copy |
| External effect fails | Retry the outbox effect independently | Do not re-run the original rule execution |
| External call times out with unknown outcome | Reconcile using destination idempotency key/status before repeating | Timeout does not prove the remote side effect failed |

ACK, NAK, delayed NAK, offset commit, and dead-letter operations belong to the broker adapter. Application services must not import NATS or Kafka client types.

## Durable retry model

Broker redelivery is a short-term transport recovery mechanism, not the only retry scheduler. Retryable business work must eventually be represented in durable application state so it survives broker delivery limits and worker restarts.

The durable retry record should include:

- tenant ID and stable event ID;
- original event envelope or a durable payload reference;
- retry state and attempt count;
- next eligible attempt time;
- classified error code and a bounded, redacted diagnostic;
- creation/update timestamps and the last claim owner/lease expiry if claims are used.

Use a unique key on tenant + event identity and an index over due work. Claim due work atomically (for PostgreSQL, a bounded query using row locking such as `FOR UPDATE SKIP LOCKED`, with lease expiry or an equivalent recovery mechanism). A worker must not mark retry work complete before the event transaction commits. Retry state transitions and event processing must be designed together to prevent two active attempts from racing.

**Important:** do not add a second retry table that duplicates `scheduled_events` without deciding which abstraction owns delayed event delivery. Inspect and either deliberately extend the existing scheduler with retry metadata/state semantics or introduce a separate retry repository with a clear single owner. The final implementation must work for both PostgreSQL and MongoDB adapters or explicitly declare one backend unsupported.

## Error classification and backoff

- **Transient infrastructure**: connection resets, temporary dependency unavailability, serialization/deadlock errors, and timeouts known to occur before commit. Use bounded exponential backoff with full jitter.
- **Permanent input/rule failures**: invalid envelope, unsupported schema, invalid fact path, deterministic evaluation/type error, or invalid destination. Persist quarantine and stop automatic retries.
- **Operationally blocked**: missing configuration, unavailable rule activation, or authorization/configuration failures. Do not silently classify every such error as poison. Define whether this is retried, paused, or quarantined with an actionable operator reason.
- **Ambiguous external outcomes**: persist an unknown/reconciliation state and query the destination or repeat only with a durable idempotency key.

Backoff should be bounded by both maximum delay and maximum elapsed retry time. Add jitter, attempt history, retry budgets, and metrics. Respect a downstream Retry-After value where the adapter provides one. Never use an unbounded in-memory timer as the only record of future work.

## Ordering and shard ownership

- Preserve the ordering contract per tenant + partition key, not globally.
- If event N for a key is waiting for retry, do not allow N+1 for that key to commit out of order unless the product explicitly supports a gap policy.
- Unrelated keys should continue to make progress.
- A lease holder must validate a monotonically increasing fencing token in the same database transaction as inbox/execution/outbox writes.
- Lease loss or queue submission failure must not silently ACK a delivery.
- A poison event must block, skip, or be operator-resolved according to an explicit per-key policy; the behavior must be observable.
- Per-process key queues alone are not a distributed ordering guarantee. Test redelivery, ownership transfer, and concurrent workers against live PostgreSQL and NATS.

## Outbox guarantees

- Rule execution and outbox creation are one database transaction.
- Effect delivery uses bounded claims, claim renewal/expiry, backoff, attempt history, and a terminal quarantine/dead state.
- Stable effect IDs are used as idempotency keys where supported.
- If the destination performs an effect but the response is lost, reconcile or retry with the same idempotency key.
- Outbox ordering is explicit. `SKIP LOCKED` by itself does not guarantee ordering for the same partition key.
- Never hold a database transaction open across an external network call.

## JetStream MaxDeliver and recovery

`MaxDeliver` is not a dead-letter queue. Before production readiness, implement and test a recovery path for exhausted deliveries, including the JetStream max-delivery advisory or another durable reconciliation mechanism. The recovery path must retain the payload and original metadata, persist quarantine/retry state before acknowledging or terminating the original delivery, and be idempotent if the advisory is delivered more than once. Alert on exhausted deliveries and unresolved recovery records.

Do not assume publishing a DLQ copy and ACKing the original is atomic. A crash between those operations can create duplicates or lose a handoff unless the recovery operation is idempotent and durably reconciled.

## Transport-neutral port requirements

The worker should consume a narrow application-facing contract for fetch and settlement. Keep vendor-specific behavior in adapters. Avoid redundant interfaces that differ only by naming; prefer consumer-owned interfaces and split capabilities only where a caller needs them.

The contract suite for every adapter must cover:

- ACK succeeds only when the adapter reports successful settlement;
- delayed retry maps to the correct broker behavior;
- malformed payload metadata remains available for quarantine;
- cancellation and shutdown do not silently ACK pending work;
- duplicate delivery is possible and safely handled;
- exhausted-delivery recovery is visible and repeatable;
- adapter-specific errors are wrapped/classified without leaking vendor types into domain packages.

## Required verification before claiming production readiness

1. Unit tests for error classification, retry schedule bounds, jitter bounds, and state transitions.
2. PostgreSQL integration tests for atomic retry handoff, claims, duplicate scheduling, concurrent workers, and lease expiry.
3. NATS JetStream integration tests for ACK/NAK/delayed retry, MaxDeliver exhaustion, advisory recovery, restart, and duplicate delivery.
4. Crash injection at each boundary: before transaction, during transaction, after commit before ACK, after external send before marking delivered, and during quarantine handoff.
5. Per-key ordering tests with one blocked key and at least one unrelated key that continues progressing.
6. Tests against both storage adapters if both remain supported.
7. Benchmarks for throughput, p95/p99 latency, retry backlog age, database load, outbox drain rate, and recovery time.

A queued CI workflow is pending, not passed. A unit test is not evidence of a real Postgres/NATS crash-recovery guarantee. Report checks against the exact tested commit.
