# FlowRule Story: from deployment to runtime

This folder turns the project into a single narrative: how FlowRule is built from scratch, how it is deployed, how it scales, and how a business event becomes a durable rule execution and effect.

## Story in one sentence

FlowRule is a durable, keyed event-processing system that accepts incoming events, pins the active rule revision for the tenant and rule set, evaluates them deterministically, records the execution in SQL, and publishes outbound effects through an outbox pipeline.

## Where the system starts

The runtime is split into two main pieces:

- API process: accepts rule deployment and event intake requests.
- Worker process: consumes durable broker messages, evaluates rules, and writes execution state.

The actual code entrypoints are:

- [cmd/api/main.go](../../cmd/api/main.go)
- [cmd/worker/main.go](../../cmd/worker/main.go)

The durable domain model is defined in:

- [internal/domain/types.go](../../internal/domain/types.go)

The broker abstraction is in:

- [internal/adapters/nats/nats.go](../../internal/adapters/nats/nats.go)

The business use cases are in:

- [internal/application/usecases.go](../../internal/application/usecases.go)

## Canonical execution path

```text
client request
  -> API / ingress
  -> durable broker (NATS JetStream)
  -> worker pull consumer
  -> inbox + activation + rule revision lookup
  -> evaluator
  -> execution + outbox insert in SQL
  -> effect publisher
  -> external destination / topic
```

## The design pillars

1. Durable event acceptance
2. Rule revision immutability
3. Transactional inbox and execution audit
4. Ordered processing per partition key
5. At-least-once broker delivery with deduplication
6. Outbox-based effect delivery with retry and quarantine

## Read the story in order

- [01-build-and-deployment.md](./01-build-and-deployment.md) — build the system from scratch
- [02-hld.md](./02-hld.md) — high-level design and operational goals
- [03-lld.md](./03-lld.md) — internal modules, ports, and transaction flow
- [04-partitions-and-scaling.md](./04-partitions-and-scaling.md) — partition model, sharding, and scale strategy
- [05-producer-consumer-and-requests.md](./05-producer-consumer-and-requests.md) — producer, consumer, request types, and runtime flow
- [06-rule-event-mapping.md](./06-rule-event-mapping.md) — event mapping to rules, actions, and outcome records

## Main design documents in the repo

The project already contains the deeper reference material:

- [docs/hld.md](../hld.md)
- [docs/lld.md](../lld.md)
- [docs/request-flows.md](../request-flows.md)
- [docs/architecture.md](../architecture.md)
- [docs/target-plan.md](../target-plan.md)
- [docs/rule-and-event.md](../rule-and-event.md)

This story collapses them into a practical deployment and runtime narrative that explains the system as a single coherent flow.
