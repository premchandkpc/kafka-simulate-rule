# Story 02 — HLD and deployment

## System purpose

FlowRule consumes a durable business event, evaluates the active version of its rule set, stores an auditable decision, and reliably schedules follow-up effects. It is not a broker, workflow engine, service discovery system, or distributed consensus system.

```text
rule administrator ──HTTP──> API ──> PostgreSQL rule registry

event producer ──publish──> NATS JetStream ──pull──> worker
                                                    │
                                                    ├─> inbox + executions + outbox (PostgreSQL)
                                                    │
                                                    └─> effect publisher ──> event topic / command destination

operator ──HTTP──> API ──> execution query
```

## Components and ownership

| Component | Current responsibility | Deployment unit | Current state |
| --- | --- | --- | --- |
| API | Compile/save/activate a rule; read an execution; health response | `cmd/api` | Prototype |
| JetStream | Durable input stream and explicit broker acknowledgement | Managed NATS or NATS cluster | External dependency |
| Worker | Pull messages, validate, evaluate, persist, ack; periodically publish outbox | `cmd/worker` | Prototype |
| PostgreSQL | Rule registry, inbox, execution audit, outbox, shard lease and quarantine schemas | Managed HA Postgres | External dependency |
| Destination | Accept an emitted event or command using an idempotency key | Separate service | Fake adapter only |

## Production topology

Deploy API, worker, and publisher independently. The code currently combines worker and publisher in one process; split them when destination capacity or failure isolation warrants it.

```text
Internet/admin network  -> ingress -> API replicas -> HA Postgres
Producer network        -> NATS cluster (JetStream replicas) -> worker replicas -> HA Postgres
                                                         \-> publisher replicas -> allowlisted destinations
```

Use TLS/mTLS between services, secret-backed connection strings, least-privilege database roles, network policies, non-root images, CPU/memory limits, a disruption budget, and separate autoscaling policies. The repository has none of these manifests yet.

## Guarantees and boundaries

The intended model is: at-least-once consumption, exactly-once *local recording* through a transactional inbox/outbox, and effectively-once external effects only when the destination deduplicates a stable `effect_id`.

The current code falls short of the local atomicity claim: inbox insert, rule read, execution insert, outbox insert, and inbox commit run in separate pool calls. A crash between calls can leave an inbox row in `processing` and cause redelivery to have no execution to return. Treat it as a development prototype until Story 05’s transaction work is complete.

Never promise exactly-once HTTP delivery: a destination may receive a request just before the publisher crashes. It must accept `effect_id` as an idempotency key and return the same result for repeats.

## Release path

1. Build and test a pinned source revision.
2. Run migrations once, with rollback/backup policy prepared.
3. Deploy API; verify health and rule compilation against a non-production tenant/scope.
4. Deploy workers with bounded concurrency; observe pending-message age and database connections.
5. Enable publishers/destinations; observe outbox age, retry rate, and quarantine count.
6. Roll back application code through the platform. Rule rollback is a separate activation change and affects only new executions.

Rule revision creation/activation is currently one API operation; the advertised revision-specific activation endpoint returns `501 Not Implemented`.
