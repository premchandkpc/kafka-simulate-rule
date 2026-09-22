# Operations

## Deployment

### Prerequisites

- PostgreSQL 14+
- NATS JetStream
- Go 1.21+

### Defaults

| Variable | Default |
|----------|---------|
| DATABASE_URL | postgres://postgres:postgres@localhost:5432/flowrule?sslmode=disable |
| NATS_URL | nats://localhost:4222 |
| MIGRATIONS_DIR | migrations |
| API listen | :8080 |
| Stream | flowrule |
| Consumer | flowrule-worker |
| Subject filter | events.> |

### Steps

1. Start Postgres and NATS
2. `go run ./cmd/api` (runs migrations automatically)
3. `go run ./cmd/worker`
4. Deploy a rule via `POST /v1/rules/{ruleSet}/revisions`
5. Publish events to NATS subject `events.{type}`

### Production checklist

- [ ] HA Postgres (streaming replication)
- [ ] JetStream with replication factor >= 3
- [ ] TLS for all connections
- [ ] Container builds with pinned revisions
- [ ] Migration job separate from app startup
- [ ] Config/secret injection (env or volume)
- [ ] Readiness/liveness probes
- [ ] Resource limits (CPU, memory, connections)
- [ ] Real destination adapters (not FakeDestination)
- [ ] Metrics and alerting

## Failure scenarios

### Crash before commit

All changes roll back. Broker redelivers. Inbox dedup catches it as a new attempt.

### Crash after commit, before ack

Inbox already committed. On redelivery, `Inbox.Get` returns the committed entry. Existing execution is returned. No duplicate effect.

### Destination down

Outbox retry with exponential backoff: 1s, 2s, 4s, 8s, 16s (capped at 60s). After `max_attempts` (default 5), effect moves to quarantine.

### Poison message

Permanent errors (invalid envelope, rule not found) are acked immediately. Transient errors trigger retry with backoff.

### Two workers same event

`FOR UPDATE SKIP LOCKED` on outbox claim ensures exactly one worker processes each effect. Inbox unique constraint prevents duplicate execution.

### Outbox publisher crash

Claimed effects have `claim_expires_at`. After TTL, effects become claimable again by another publisher.

## Partitioning

### Model

4096 virtual shards mapped to physical workers via `shard_leases` table. Each lease has a fencing token to prevent stale workers from committing.

### Ordering

Events with the same `partition_key` route to the same shard. Ordering is guaranteed within a key, never across keys.

### Scaling

| Dimension | Scales by |
|-----------|-----------|
| API | Request rate (horizontal replicas) |
| Worker | Backlog depth (horizontal replicas) |
| Publisher | Outbox send rate (independent) |
| PostgreSQL | Connection pooling, read replicas |
| NATS | Stream consumers, cluster nodes |

### Hot keys

A single key with high volume becomes a bottleneck on one shard. Ordering remains correct; the rest of the system keeps running. Mitigate by choosing `partition_key` as the smallest entity that needs ordering.

### Rebalancing

1. Mark instance draining
2. Stop new fetches
3. Finish bounded in-flight executions
4. Release/expire leases
5. Terminate

## Metrics

- Accepted/rejected events per tenant
- Duplicate inbox conflicts
- Evaluation latency p50/p99
- Broker lag (oldest unacked message age)
- Outbox age by status
- Quarantine count by error class
- Effect delivery latency
- Lease loss rate
