# FlowRule Architecture Map

This is a source-backed map of the current implementation, not a target-state promise. Runtime code and tests take precedence over diagrams, roadmaps, and dated audit snapshots.

## Runtime topology

~~~mermaid
flowchart LR
  Producer[Producer / Go SDK] -->|events.shard.N| NATS[NATS JetStream]
  NATS --> Worker[cmd/worker]
  Worker --> App[application.Worker]
  App --> Queue[runtime/keyqueue]
  App --> Lease[runtime/shard lease manager]
  Lease --> LeaseDB[(Shard lease repository)]
  Queue --> Events[services/events]
  Events --> Tx[Transactional storage]
  Tx --> Inbox[(Inbox)]
  Tx --> Rules[(Rule revisions and activations)]
  Tx --> Executions[(Executions)]
  Tx --> Outbox[(Outbox effects)]
  Outbox --> Effects[services/effects]
  Effects --> Router[Effect router]
  Router -->|emit event| NATS
  Router -->|command| Destination[Configured HTTP destination]
  Scheduler[runtime/scheduler] --> Scheduled[(Scheduled events)]
  Scheduler -->|events.shard.N| NATS
  API[cmd/api] --> HTTP[HTTP router and handlers]
  HTTP --> Services[Use-case services]
  Services --> Storage[internal/storage]
  Storage --> PG[(PostgreSQL default)]
  Storage --> Mongo[(MongoDB option)]
~~~

## Process startup

### API — cmd/api/main.go

1. Load and validate configuration.
2. Open storage using internal/storage and initialize the selected backend/migrations.
3. Create rule compiler and the rule, workflow, batch, and quarantine services.
4. Create the HTTP router and register rule, workflow, contract, batch, quarantine, scheduled-event and health handlers.
5. Serve HTTP and shut down when the process context is cancelled.

The API currently starts HTTP only. GRPC_ADDR is not an active listener. The /health route is liveness only and does not check storage or NATS readiness. There is no authentication/authorization; tenant extraction reads caller-supplied header/query data and falls back to default, so this process must not be exposed to untrusted users.

### Worker — cmd/worker/main.go

1. Load config, start observability, and open/initialize storage.
2. Construct a NATS JetStream consumer with base subject events and the configured shard count. The adapter declares filters for events.shard.N.
3. Construct the compiler/evaluator, transactional event service, external sender and effect router.
4. Construct batch service, shard lease manager, scheduler and application.Worker.
5. Start scheduler/worker loops; on shutdown stop loops and close consumer, storage, and observability.

The effect router is part of the runtime wiring, not an optional helper: emitted child events must be published to events.shard.N using the same NUM_SHARDS as the consumer. The event type belongs in EventEnvelope.Type. Non-event command effects use the configured external destination. In development/test only, an absent effect URL may select a fake destination; other environments must fail startup if the URL is absent.

### CLI processes

- cmd/codegen is a DB-backed command that retrieves a registered contract and generates Go, Java, Protobuf or JSON Schema artifacts. It uses flags rather than subcommands; it is not a YAML-input generator.
- cmd/migrate currently supports up only. There are no implemented down/status operations.

## Event processing sequence

~~~mermaid
sequenceDiagram
  participant P as Producer
  participant N as NATS JetStream
  participant W as Worker and key queue
  participant S as Event service
  participant D as Transactional storage
  participant O as Outbox publisher
  P->>N: publish EventEnvelope to events.shard.N
  N->>W: deliver event for owned shard
  W->>W: validate envelope and obtain fencing token
  W->>S: Process(envelope, token, shard, worker)
  S->>D: begin transaction and assert lease/fencing
  S->>D: inbox lookup/insert
  S->>D: load activation and immutable revision
  S->>S: deterministic evaluation
  S->>D: write execution and outbox effects
  S->>D: mark inbox committed
  S->>D: commit
  S-->>W: committed execution
  W->>N: ACK after successful commit
  O->>D: claim pending effects
  O->>N: republish emitted child events
  O->>D: settle or retry effect state
~~~

The event service keeps fencing validation, deduplication, evaluation persistence, execution state, outbox writes and inbox commit state within one transaction. A conflicting inbox insert with no corresponding execution must fail closed; never create a second execution to hide inconsistent state.

## Child-event routing contract

- Broker base subject is events.
- NATS subjects are events.shard.N, where N is EventEnvelope.VirtualShard(NUM_SHARDS).
- EventEnvelope.Type carries business event type such as payment.requested; it is not part of the configured broker subject.
- The effect router and scheduler must use the same base subject and shard count as the stream/consumer. A publish to a different topic does not reach consumers if it is outside the stream subject filter.
- External command effects go to the configured HTTP destination. Forward the stable idempotency key and keep outbound timeouts/destination validation in scope.
- Delivery is at-least-once. External idempotency depends on the receiving endpoint, and the overall system must not be described as exactly-once.

## Package and file responsibilities

| Package/path | Responsibilities |
|---|---|
| cmd/api | API composition root and lifecycle |
| cmd/worker | Worker composition root, NATS/effect wiring and shutdown |
| cmd/codegen | Database-backed contract code generation CLI |
| cmd/migrate | Apply SQL migrations |
| internal/config | Environment parsing, raw value parsing and validation |
| internal/domain | Envelopes, rules, predicates, actions, decisions, workflow/batch/schedule/lease types, validation, IDs and hashes |
| internal/ports | Broker, delivery, transaction, repository, cache, runtime and use-case interfaces |
| internal/rules | Rule compilation, validation and deterministic predicate/action evaluation |
| internal/services/events | Fencing, inbox dedup, rule lookup/evaluation, execution/outbox transaction |
| internal/services/effects | Outbox claim, dispatch, retry/backoff, quarantine coordination |
| internal/services/effects/router.go | Child-event broker publishing versus external command dispatch |
| internal/services/rules | Rule revision writes, activation CAS and rule queries |
| internal/services/batches | Batch formation, processing and query APIs |
| internal/services/workflow | Workflow instance transitions; definition helper methods currently contain placeholders |
| internal/services/quarantine | Quarantine query and replay-marking operations |
| internal/application/worker.go | Worker loops, dispatch, periodic publishing/batching, shard consumer reconciliation |
| internal/runtime/keyqueue | Bounded global work, per-key FIFO processing and hot-key checks |
| internal/runtime/shard | Lease acquisition/renewal, owned-shard tracking and fencing tokens |
| internal/runtime/scheduler | Claim due schedules and publish events |
| internal/storage/storage.go | PostgreSQL/Mongo selection and repository composition |
| internal/factory/factory.go | Legacy broad Mongo-focused factory; current entrypoints use internal/storage |
| internal/adapters/http | Router, request handlers, middleware, JSON/YAML decoding and responses |
| internal/adapters/sql | PostgreSQL pool/transactions and repository queries |
| internal/adapters/mongo | Mongo client/session/transaction and repositories |
| internal/adapters/nats/jetstream.go | Stream configuration, sharded consumers, delivery ACK/NAK/retry and publishing |
| internal/adapters/redis | Redis client and cache implementation; not wired into the primary event path |
| internal/adapters/effects/fake.go | Fake/HTTP destinations plus helper clock/ID types; responsibilities are mixed |
| internal/adapters/memory | In-memory contract registry |
| internal/observability | Logging, metrics, tracing and HTTP handler |
| internal/codegen/generator.go | Go/Java/Protobuf/JSON Schema and runtime template generation |
| internal/services and internal/runtime tests | Unit/regression tests for application behavior |
| api/*.json | Public envelope/rule JSON schema source files |
| migrations | Numbered SQL migrations; no down-script set |
| sdk/go/runtime | Original standalone envelope, shard, producer, consumer and evaluation helper module |
| sdk/go/flowrule | Separate client SDK module with configuration, contracts, local/HTTP/gRPC/NATS transports and a basic example |
| examples/{banking,ecommerce,food-delivery} | Independent modules with contract/rule/workflow sources and sample applications |
| tests/contract | In-memory repository contract suites, not full SQL/Mongo conformance |
| tests/integration | Service behavior tests using mocks/in-memory repositories, not live infrastructure tests |
| docs/01-*.mmd to docs/27-*.mmd | Focused flow and design diagrams |
| docs/audit | Timestamped static review findings that must be revalidated |

More detailed file-by-file instructions, build commands and invariants are in AGENTS.md.

## Dependency direction

~~~mermaid
flowchart TD
  Entrypoints[cmd/* composition roots] --> Services[internal/services]
  Entrypoints --> Runtime[application and runtime]
  Entrypoints --> Storage[storage and adapters]
  HTTP[HTTP adapter] --> Ports[ports]
  Services --> Ports
  Services --> Domain[domain]
  Rules[rules compiler/evaluator] --> Domain
  Ports --> Domain
  Storage --> SQL[SQL adapter]
  Storage --> Mongo[Mongo adapter]
~~~

Rules:
- Domain and rules never import transport, adapters, services, or database drivers.
- Use-case services depend on domain and ports, not concrete adapters.
- Adapters implement interfaces and keep infrastructure concerns at the edge.
- Keep only one preferred composition route. internal/factory and internal/storage currently overlap; verify references before deleting the legacy factory.
- The transaction port still exposes transaction context/fencing details and TxRepos groups many repositories. These are recognized debt, not a pattern to expand.
- SDK generated code must not import unexported/internal server packages.

## Confirmed gaps and next steps

| Priority | Gap | Next step |
|---|---|---|
| P0 | No authentication/authorization or trusted tenant identity | Add verified identity, tenant/resource authorization and cross-tenant denial tests before public exposure |
| P1 | Outbox ScheduleRetry/Quarantine not claimant-fenced; settlement errors sometimes logged and swallowed | Fence all state transitions; return durable-state failures and test expired claims/competing publishers |
| P1 | Sequential effect dispatch and fixed claim TTL | Add bounded concurrency and per-effect timeouts after claimant semantics are safe |
| P1 | Transaction context/fencing leaks into the ports; TxRepos too broad | Introduce use-case-owned unit-of-work contracts with atomicity regression tests |
| P1 | Contract tests are in-memory-only | Reuse suites against actual SQL and Mongo adapters and test transaction/unique-key semantics |
| P1 | HTTP request body decoding lacks size and strict trailing-content guard | Bound reads, reject trailing documents, and use authenticated tenant context |
| P2 | Workflow service contains not-implemented definition methods | Implement intentionally or remove placeholders and use a clear repository/service boundary |
| P2 | internal/factory duplicates storage composition | Verify consumers and consolidate composition |
| P2 | health is liveness only and no gRPC listener exists | Separate readiness/liveness; wire/remove misleading configuration |
| P2 | No live crash/recovery suite or benchmark evidence | Add live PostgreSQL/NATS failure injection and reproducible benchmarks |
| P2 | SQL outbox insertion is one insert per effect | Measure throughput then introduce batch insert if warranted |

Do not turn this table into claims of completion without changes and tests. A follow-up audit should identify the reviewed main SHA and workflow runs, and label each old finding fixed/partial/open against current source.
