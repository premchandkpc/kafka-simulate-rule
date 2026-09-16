# FlowRule Target Plan

## Decision summary

FlowRule is a durable, keyed event-to-decision processor. It evaluates an immutable rule revision and records a decision plus durable effects. It is not a broker, RPC framework, workflow engine, service registry, or general-purpose VM.

The v1 deployment has four runtime components:

```text
producer -> broker -> worker -> SQL transaction -> outbox publisher -> destinations
                         |             |
                         +-- registry -+-- audit/replay
```

- Broker delivery is at least once.
- SQL provides the atomic boundary for inbox, revision pinning, execution audit, owned state, and outbox insertion.
- Effects are effectively once only when the destination honors `effect_id` as an idempotency key.
- Ordering is guaranteed per `(tenant_id, partition_key)` for a rule stream, not globally.
- Replays are new delivery attempts of the same event identity unless explicitly cloned by an operator.

## What changed from the prototype

| Prototype capability | Target treatment | Reason |
| --- | --- | --- |
| Rust bytecode VM and CGo bridge | Remove from v1; use a bounded typed evaluator | Fewer failure boundaries and easier replay |
| Raft, gossip, peer bus | Remove; use broker ownership and SQL leases where needed | Platform infrastructure already solves membership |
| Plan broadcast and quorum activation | Replace with immutable SQL revisions and one activation transaction | Source of truth is queryable and auditable |
| Priority lanes and work stealing | Replace with bounded worker pools and tenant quotas | Backpressure is explicit and measurable |
| Service registry and arbitrary service calls | Replace with allowlisted effect destinations | Rules remain pure and bounded |
| Saga in the runtime | Model compensations as explicit business events | Avoid pretending external calls are ACID |
| File execution state | Replace with SQL inbox/outbox and object storage for large payloads | Crash recovery and operations need durable shared state |
| Compact DSL | Defer; accept JSON/YAML schema first | Schema can be validated and versioned before syntax is optimized |

## Bounded contexts

- **Rule management:** draft, validate, publish immutable revision, activate revision.
- **Event intake:** validate envelope, apply admission policy, publish or accept durable event.
- **Decisioning:** pure evaluation of event and declared fact snapshot.
- **Effect delivery:** claim outbox rows, call destinations, retry, quarantine.
- **Operations:** inspect, replay, quarantine, retention, audit export.

Each context owns its invariants. HTTP handlers, broker clients, and database adapters do not contain domain rules.

## Partition and concurrency model

The producer supplies the business key whose transitions must be ordered. The routing key is not selected by the worker and is never silently changed by a rule.

```text
virtual_shard = hash(tenant_id || ":" || partition_key) mod N
```

Use a fixed virtual shard count, for example 4096, and assign virtual shards to workers. Changing worker count changes ownership, not the hash space. A worker may process different shards concurrently, but processes one key serially. A hot key is intentionally serialized and must be visible as a metric.

The broker adapter maps the same key to its native partitioning:

- JetStream: subject or stream consumer configuration plus deterministic key routing.
- Kafka: record key and consumer-group partition ownership.
- Managed queue: message group/session key where supported.

The application never assumes that native partition numbers are stable identifiers.

## Control plane

The API owns rule lifecycle and operations:

- `POST /v1/rules/{ruleSet}/revisions`
- `POST /v1/rules/{ruleSet}/revisions/{revision}/activate`
- `POST /v1/events` for authenticated synchronous acceptance
- `GET /v1/executions/{executionID}`
- `POST /v1/replays`
- `POST /v1/quarantine/{id}/replay`

Activation validates the compiled artifact, compatibility, limits, destination allowlist, and authorization before one SQL transaction updates the activation pointer. Workers fetch a revision on cache miss; cache invalidation is an optimization, never a correctness dependency.

## Non-goals

- Kafka-compatible broker implementation or custom storage engine.
- Arbitrary loops, plugins, network calls, timers, or workflows inside rules.
- Global ordering, cross-tenant transactions, or distributed exactly-once.
- Cross-database two-phase commit.
- A custom scheduler, membership protocol, or consensus layer.

## Success criteria

The first production candidate must prove:

1. Duplicate broker delivery creates one execution and one effect row.
2. A crash after SQL commit and before broker acknowledgement is harmless.
3. Same event, revision, and fact snapshot produce the same decision hash.
4. A worker loss restores backlog without manual state repair.
5. A tenant or hot key cannot exhaust shared database and destination capacity.
6. An operator can trace event -> revision -> decision -> effect -> delivery attempts.
