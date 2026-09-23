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

The following are hardcoded in cmd/worker/main.go (not environment variables):

| Setting | Value | Description |
|---------|-------|-------------|
| STREAM | flowrule | JetStream stream name |
| CONSUMER | flowrule-worker | JetStream consumer name |
| SUBJECTS | events.> | Subject filter for event consumption |
| ACK_WAIT | 30s | NATS ack wait timeout |
| MAX_DELIVER | 10 | Max delivery attempts before NATS requeues |
| PUBLISH_INTERVAL | 5s | Effect publisher batch interval |
| PUBLISH_BATCH_SIZE | 10 | Effects per publish batch |

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

**Note**: Shard tables and fencing token logic exist in migrations and domain, but the worker does not yet enforce shard ownership or filter by virtual shard.

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

Currently no metrics are exported. The following should be added:

- Accepted/rejected events per tenant
- Duplicate inbox conflicts
- Evaluation latency p50/p99
- Broker lag (oldest unacked message age)
- Outbox age by status
- Quarantine count by error class
- Effect delivery latency
- Lease loss rate

## Troubleshooting

### Event not processed

1. Check worker logs for errors
2. Verify NATS consumer is running: `nats stream info flowrule`
3. Check inbox table for stuck entries: `SELECT * FROM inbox WHERE status = 'processing'`
4. Verify rule activation exists: `SELECT * FROM rule_activations`

### Effects not delivered

1. Check outbox_effects table for pending/claimed status
2. Verify destination is reachable
3. Check quarantine table for failed effects
4. Increase PUBLISH_INTERVAL / PUBLISH_BATCH_SIZE if backlog growing

### Duplicate executions

1. Verify inbox unique constraint exists
2. Check that broker ACK happens after commit (worker code order)
3. Verify NATS MaxDeliver and AckWait settings

### High latency

1. Check PostgreSQL connection pool saturation
2. Check NATS consumer fetch batch size
3. Look for long-running transactions in pg_stat_activity
4. Consider read replicas for activation/rule queries