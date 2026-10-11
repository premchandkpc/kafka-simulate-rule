# Random Producer Example

This example generates random FlowRule event envelopes and publishes them to the same NATS JetStream subject family used by the worker. It is deliberately located under `examples/`: it demonstrates producer-to-broker integration and can be run against a real local stack. It is not part of API/worker startup or a replacement for consumer-side end-to-end tests.

## Run against the local stack

From the repository root:

```sh
cp .env.example .env
docker compose up -d postgres nats redis api worker
docker compose --profile examples run --rm random-producer
```

Or run it directly after starting NATS and the worker:

```sh
go run ./examples/random-producer --clients 10 --max-messages 100 --shards 4096 --concurrency 16 --nats nats://localhost:4222
```

Configuration:
- `RANDOM_PRODUCER_CLIENTS`: number of synthetic tenant identities (1-1000), default 10.
- `RANDOM_PRODUCER_MESSAGES_MAX`: maximum messages per identity (1-100), default 100.
- `RANDOM_PRODUCER_CONCURRENCY`: concurrent publishers (1-256), default 16.
- `NUM_SHARDS`: must match the worker's shard count, default 4096.
- `NATS_URL`: NATS endpoint; inside Compose use `nats://nats:4222`.

The example reports generated attempts, JetStream-acknowledged publications, and failures. Broker acknowledgement means the broker accepted a message; it does not prove that a worker processed it. Use integration/E2E tests to assert durable execution, ACK/redelivery behavior, inbox deduplication and lease failover.

## Ownership and boundaries

- `main.go` parses options, configures shutdown/deadline handling, and wires the NATS adapter.
- `producer.go` owns example-specific generation and the narrow publisher contract.
- `producer_test.go` verifies generated envelopes, domain-computed shard subjects, invalid settings, cancellation and publisher failures without requiring a live broker.
- The example reuses `internal/domain.EventEnvelope` and `VirtualShard`; it does not reimplement the production schema or hash algorithm.
- The transport implementation remains in `internal/adapters/nats`. No NATS types leak into the producer use case.

This is an example/load tool, not production runtime functionality. If a future reusable production use case needs load generation, promote it into an internal package only after a concrete non-example caller exists.
