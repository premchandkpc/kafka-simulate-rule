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

## Documents

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
