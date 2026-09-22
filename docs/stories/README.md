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

The executable is a vertical-slice prototype, not yet a production deployment. In particular, it has no container/Kubernetes manifests, no real outbound destination, no persistent quarantine implementation, no worker shard ownership, no request authentication, and its inbox/execution/outbox writes are separate SQL calls rather than one transaction. The stories do not hide these limitations.
