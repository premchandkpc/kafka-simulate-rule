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
