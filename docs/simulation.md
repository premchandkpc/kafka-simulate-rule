# Random producer and cluster simulation

The opt-in simulator publishes random event envelopes into the same NATS JetStream subject family consumed by the workers. It is a load generator, not a substitute for end-to-end consumer acknowledgement verification.

## Run

```sh
cp .env.example .env
docker compose up -d postgres nats redis api worker
docker compose --profile simulation run --rm simulator
```

The default run creates 10 simulated tenant/client identities. Each identity publishes a random number of messages from 1 through 100. Tune the run with `SIM_CLIENTS`, `SIM_MESSAGES_MAX` (1-100), `SIM_CONCURRENCY` (1-256), and `NUM_SHARDS`. The shard count must match the running API/workers. The simulator connects to `nats://nats:4222`, using Compose DNS rather than localhost.

A direct run is also available:

```sh
go run ./cmd/simulator --clients 10 --max-messages 100 --shards 4096 --concurrency 16 --nats nats://localhost:4222
```

## Message and routing contract

- Each message has a unique event ID, tenant/client ID, random partition key, timestamp, event type, JSON data, and simulator source header.
- The simulator uses the same FNV-1a hash over `tenant_id + ":" + partition_key` as `domain.EventEnvelope.VirtualShard`.
- Messages publish to `events.shard.N`; `N` is computed with the configured shard count.
- Publish success means JetStream acknowledged broker acceptance. It does **not** mean a worker finished processing the event.
- Worker consumer ACKs are separate: workers must ACK only after the event's durable transaction commits. A failure before ACK should permit redelivery, and the inbox must prevent duplicate execution.

## Service discovery and worker roles

Inside Compose, services resolve by DNS names such as `nats`, `postgres`, and `worker`. Producers publish to the broker service, not directly to a worker pod. JetStream dispatches messages to consumers; workers use shard leases and fencing tokens to determine which shard work they may process. This avoids coupling producer routing to ephemeral pod IPs.

The current lease manager implements fixed-replica shard ownership, not a cluster-wide master election or dynamic membership protocol. There is no single elected master/follower control plane in this change. Treat lease owner as the authority for a given shard; do not infer that one worker is the global master. Dynamic membership, lease fencing under process pauses, leader failover, and consumer ACK/redelivery need live infrastructure failure tests before production claims.

## What to verify

1. Run with three workers and inspect logs for the distinct worker IDs and shard ownership.
2. Confirm simulator logs report `broker_acked` equal to `attempted` and `failed=0`.
3. Stop one worker while publishing. Verify messages for its shards remain available and are processed after a valid owner acquires the leases.
4. Kill a worker after durable commit but before ACK. Verify redelivery does not create a second execution.
5. Restart the worker and verify lease expiry/fencing prevents stale ownership from processing.
6. Repeat with a mismatched `NUM_SHARDS` only in an isolated test environment to verify configuration validation rejects it.

The simulator currently verifies producer-to-broker publication only. The checks above require consumer-side metrics, durable execution inspection, or dedicated end-to-end tests.
