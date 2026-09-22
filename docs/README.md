# FlowRule: a smaller distributed rules engine

The revised documents listed first are the authoritative target plan. The original documents remain below as design history and prototype migration context; where they differ, follow the revised set.

## Decision

Build a **durable, keyed event processor with versioned rules**, not a new Kafka, workflow platform, RPC framework, scheduler, service mesh, and VM at once.

FlowRule accepts an event, selects the active rule revision, evaluates bounded predicates and actions, and records the outcome durably. It preserves ordering for one business key and scales by assigning independent keys to partitions.

Use **NATS JetStream** as the default transport. It offers durable streams, pull consumers, acknowledgements, backpressure, replay, and a low operational burden. Keep Kafka as an adapter where its existing estate, retention requirements, or ecosystem justify it. FlowRule is a rules execution layer, not a Kafka replacement.

## Why

The current prototype overlaps Kafka/gRPC/memory buses, Raft and gossip, a Go scheduler plus Rust VM, plan distribution, service registry, and a workflow DSL. That creates too many correctness boundaries before the core rule path is proven.

The baseline has only three durable facts:

1. A rule revision was activated.
2. An input event was accepted.
3. An idempotent effect was committed or scheduled for retry.

Everything else is derived, cached, or replaceable.

## Implementation Status

### Phase 0: Contracts and Proof — DONE

- [x] Event envelope with id, type, tenant_id, partition_key, occurred_at, data, headers
- [x] Rule JSON Schema for validation
- [x] Decision/effect model with deterministic hash
- [x] Error taxonomy with ErrorClass classification
- [x] Effect identity: SHA-256(tenant_id | event_id | rule_set | revision | rule_id | action_index)
- [x] Deterministic evaluator fixtures and adapter contract fixtures

### Phase 1: Single-Process Vertical Slice — DONE

- [x] Immutable rule revisions and activation
- [x] PostgreSQL migrations (7 tables)
- [x] Inbox with duplicate detection
- [x] Execution audit trail
- [x] Outbox pattern with retry and quarantine
- [x] Pure evaluator (no I/O)
- [x] Fake destination adapter
- [x] NATS JetStream broker adapter
- [x] API server for rule deployment
- [x] Worker with fetch/evaluate/ack loop

### Phase 2: Production-Shaped Distribution — TODO

- [ ] Virtual shards and worker ownership
- [ ] Fencing tokens and lease management
- [ ] Bounded concurrency and graceful drain
- [ ] Metrics, traces, and health endpoints
- [ ] Tenant quotas
- [ ] Deploy/activate/inspect/replay APIs

### Phase 3: Operational Hardening — TODO

- [ ] Destination allowlists
- [ ] Outbox circuit breakers
- [ ] Retry policy with exponential backoff
- [ ] Quarantine tooling
- [ ] Retention policy
- [ ] Object storage for large payloads
- [ ] Authorization (RBAC)
- [ ] Backups/PITR
- [ ] Dashboards and alerts

### Phase 4: Kafka Adapter — TODO

- [ ] Kafka transport adapter
- [ ] Conformance tests

## Documents

### Code-verified system stories

- [System stories: build, HLD, LLD, flows, rules, partitioning, and scaling](stories/README.md)

- [Request flows](request-flows.md)
- [Rule and event model](rule-and-event.md)
- [Target plan and prototype simplification](target-plan.md)
- [Transport alternatives](transport-alternatives.md)
- [Data model and consistency](data-model-and-consistency.md)
- [Rule runtime contract](rule-runtime.md)
- [Project structure and standards](project-structure.md)
- [Implementation roadmap](implementation-roadmap.md)
- [Target architecture](architecture.md)
- [Rule model and correctness contract](rule-model.md)
- [Delivery plan](delivery-plan.md)
- [High-level design](hld.md)
- [Low-level design](lld.md)
- [Engineering principles and standards](engineering-standards.md)

## Non-goals for v1

- Kafka-compatible broker protocol or broker storage engine.
- General workflow/DAG language, arbitrary loops, or distributed VM.
- Exactly-once delivery to arbitrary external systems.
- In-process leader election, gossip, plan broadcast, or service registry.



# FlowRule system stories

This is the code-verified guide to FlowRule as of this repository revision. It is deliberately separate from the earlier design documents: each story labels the difference between the **current prototype** and the **production design**.

| Story | Answers |
| --- | --- |
| [01 — Build and local bootstrap](01-build-and-bootstrap.md) | What must exist, how to build/run it, and what starts automatically |
| [02 — HLD and deployment](02-hld-and-deployment.md) | System boundaries, production topology, ownership, and guarantees |
| [03 — Request and event journeys](03-request-and-event-journeys.md) | Every request/message type and its end-to-end path |
| [04 — Rules and event mapping](04-rules-and-event-mapping.md) | How an event selects, evaluates, and emits rules/actions |
| [05 — LLD, data, and failure handling](05-lld-data-and-failures.md) | Code structure, persistence, idempotency, retries, and known gaps |
| [06 — Partitioning and scaling](06-partitioning-and-scaling.md) | Ordering key choice, shards, consumers, capacity, and rollout plan |

Read stories 01–04 for an onboarding view. Read 05–06 before operating or extending the worker.

## Current-state warning

The executable is a vertical-slice prototype, not yet a production deployment. In particular, it has no container/Kubernetes manifests, no real outbound destination, no persistent quarantine implementation, no worker shard ownership, and no request authentication. The inbox/execution/outbox writes are now inside a single Postgres transaction (fixed), but the outbox claim race, durable quarantine, MaxDeliver ceiling, shard routing, and the `tenant_scope` bug remain. The stories do not hide these limitations.



# FlowRule Story: from deployment to runtime

This folder turns the project into a single narrative: how FlowRule is built from scratch, how it is deployed, how it scales, and how a business event becomes a durable rule execution and effect.

## Story in one sentence

FlowRule is a durable, keyed event-processing system that accepts incoming events, pins the active rule revision for the tenant and rule set, evaluates them deterministically, records the execution in SQL, and publishes outbound effects through an outbox pipeline.

## The two flows that matter

FlowRule has two different operational flows, and they are not the same thing:

1. Deployment flow: startup, migration, process wiring, rule activation, readiness.
2. Runtime flow: one event entering the system, being deduplicated, evaluated, committed, and turned into durable effects.

The deployment flow runs on release time and startup time. The runtime flow runs on event time and is the correctness-critical path.

This story explicitly separates the target behavior from the current implementation, because the repo contains both: a strong target architecture and a current codebase that is still catching up to it.

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

- [01-build-and-deployment.md](./01-build-and-deployment.md) — deployment flow: build, infra, migration, API, worker startup
- [02-hld.md](./02-hld.md) — high-level design and operational goals
- [03-lld.md](./03-lld.md) — internal modules, ports, and transaction flow
- [04-partitions-and-scaling.md](./04-partitions-and-scaling.md) — partition model, sharding, and scale strategy
- [05-producer-consumer-and-requests.md](./05-producer-consumer-and-requests.md) — runtime flow: event intake, broker flow, dedupe, ack/commit ordering
- [06-rule-event-mapping.md](./06-rule-event-mapping.md) — event mapping to rules, actions, and outcome records
- [07-target-vs-current-code.md](./07-target-vs-current-code.md) — explicit contract gap analysis: what the target plan says vs what the current code does

## Main design documents in the repo

The project already contains the deeper reference material:

- [docs/hld.md](../hld.md)
- [docs/lld.md](../lld.md)
- [docs/request-flows.md](../request-flows.md)
- [docs/architecture.md](../architecture.md)
- [docs/target-plan.md](../target-plan.md)
- [docs/rule-and-event.md](../rule-and-event.md)

This story collapses them into a practical deployment and runtime narrative that explains the system as a single coherent flow.
