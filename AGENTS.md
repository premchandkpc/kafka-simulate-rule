# AGENTS.md — FlowRule Engineering Guide

This file describes the repository as it exists today. Source code, configuration, and tests are authoritative. Diagrams, roadmaps, old audits, and implementation plans may describe future work or past bugs. Verify claims before presenting them as implemented.

## 1. Product and message-processing model

FlowRule is a Go event-driven rules runtime. The repository is one root Go module with an HTTP API, a worker, and command-line tools. The Go SDK and each example app are separate Go modules.

The intended event path is:

1. Producers publish an EventEnvelope to the NATS JetStream subject family events.shard.N. N is computed from tenant ID and partition key using the shared shard-count setting.
2. The worker obtains shard leases and fencing tokens, creates consumers for shards it owns, and fetches messages.
3. The bounded key queue serializes deliveries for the same partition key and caps total work across keys.
4. Event processing validates the envelope and fencing token, checks/inserts the inbox record, loads the active rule revision, evaluates it, and writes execution plus outbox effects in the same storage transaction.
5. The worker ACKs a delivery only after successful durable processing. Failures must follow the retry/quarantine policy; never ACK work whose durable outcome is unknown.
6. The effects publisher claims outbox entries. Emit-event effects return to the broker subject family events.shard.N; external command effects go to the configured HTTP destination.
7. The scheduler claims due scheduled events and publishes them to the same broker subject family. Batch processing is a separate periodic worker loop.

Transport is at-least-once. Inbox deduplication and transactional outbox semantics help protect the state transition, but external effects are effectively-once only if their receiver honors the stable idempotency key. Never claim exactly-once delivery.

## 2. Build and verification commands

Run root-module checks:

~~~sh
go build ./cmd/...
go test -race -count=1 ./...
go vet ./...
~~~

Both SDK modules and each example module must be checked separately because nested go.mod files form module boundaries:

~~~sh
(cd sdk/go && go test -race -count=1 ./...)
(cd sdk/go/flowrule && go test -race -count=1 ./...)
(cd examples/banking && go test -race -count=1 ./...)
(cd examples/ecommerce && go test -race -count=1 ./...)
(cd examples/food-delivery && go test -race -count=1 ./...)
~~~

Other useful commands:
- make build builds API, worker and codegen binaries into bin.
- make test-unit tests the internal packages.
- make test-integration runs the tests in tests/integration.
- make lint, make vet, make fmt, and make check-fmt handle linting, static analysis, formatting and format verification.
- make docker-up / make docker-down manage the local Compose stack.
- make migrate-up applies pending SQL migrations.
- make generate requires CONTRACT, CONTRACT_VERSION, TARGET and OUTPUT_DIR and works from a contract already registered in storage.

Do not claim tests/integration is a live infrastructure test: current tests use mock/in-memory repositories. Contract tests in tests/contract also exercise in-memory implementations; they are not yet a full conformance suite for PostgreSQL and MongoDB. Check the workflow run status before saying CI passed.

## 3. Process wiring and behavior

### HTTP API

- Entry point: cmd/api/main.go.
- Loads internal/config, opens and initializes internal/storage, constructs compiler and use-case services, registers HTTP handlers, and starts the HTTP server. The default HTTP bind is loopback; Compose publishes service ports on host loopback only.
- Route registration: internal/adapters/http/router.go.
- Handlers: internal/adapters/http/handlers/rules.go, workflows.go, contracts.go, batch_quarantine.go and scheduled.go.
- Middleware and request body decoding: internal/adapters/http/middleware/middleware.go and internal/adapters/http/handlers/utils.go.
- Workflow definition routes are under /v1/workflows/definitions, not /v1/workflow-definitions.
- /health is liveness only; it does not check storage or broker readiness.
- The API starts HTTP only. No gRPC server is implemented; the client SDK's gRPC transport targets a compatible server deployed separately.

SECURITY BLOCKER: no authentication/authorization is implemented. TenantExtractor accepts X-Tenant-ID, then tenant_id query parameter, then the fallback value default. All of these are caller-controlled, not authenticated identity. Do not expose this API to untrusted networks or treat tenant data access as isolated until identity and resource authorization are implemented.

### Worker

- Entry point: cmd/worker/main.go.
- Loads config, initializes observability and storage, constructs the NATS JetStream consumer, event/effect/batch services, lease manager, scheduler, and application.Worker.
- NATS adapter: internal/adapters/nats/jetstream.go. The worker subject base is events; consumers filter events.shard.N.
- Runtime loop and polling: internal/application/worker.go.
- Bounded per-key queue and hot-key observation: internal/runtime/keyqueue/queue.go.
- Lease ownership, renewal, and fencing cache: internal/runtime/shard/lease.go.
- Scheduled release: internal/runtime/scheduler/scheduler.go.
- The effect sender must be the configured effects.Router: it routes emitted child events to the same events.shard.N subjects and non-event command effects to an external sender. A router that exists but is not wired into cmd/worker does not provide event chaining.
- In development/test, the worker can use FakeDestination if EFFECT_DESTINATION_URL is empty. In other environments, missing EFFECT_DESTINATION_URL must fail startup.

### CLI tools

- cmd/codegen/main.go is a database-backed CLI. It reads a registered contract and generates Go, Java, Protobuf, or JSON Schema artifacts. Flags are -contract, -version, -target, -output, -list, -dsn and -migrations. It does not accept a YAML file path or implement a generate --all subcommand.
- cmd/migrate/main.go currently supports only the up command. Down/status are not implemented because there are no paired reversible migration scripts. Do not advertise rollback as supported.
- CLI entrypoints must return errors from run functions so deferred resources close before main exits. Avoid log.Fatal/log.Fatalf/os.Exit inside resource-owning run functions.

## 4. Dependency direction and package boundaries

Keep dependencies moving inward:

- cmd/* packages are composition roots. They load configuration, construct concrete adapters, wire use cases, manage process lifecycle, and then exit.
- internal/domain contains business types, validation, state transitions, IDs, deterministic hashes and domain errors. It must not import adapters, services, transport libraries or storage drivers.
- internal/rules compiles and evaluates rules. Evaluation must remain deterministic, side-effect-free, and clock-injected; do not add I/O or randomness.
- internal/services/* contains use-case orchestration. It should depend on domain and consumer-owned ports, not concrete SQL, Mongo, NATS or HTTP packages.
- internal/application coordinates worker runtime. internal/runtime/* owns bounded queues, shard leases and scheduler loops.
- internal/ports declares broker, repository, transaction, runtime and service contracts. Prefer small interfaces owned by the consumer. Current transaction abstractions still expose adapter lifecycle details and TxRepos is broad; treat those as technical debt, not a pattern to expand.
- internal/adapters/* implements I/O and translates between ports/domain and transport or persistence details.
- internal/storage selects the configured backend and composes repositories. Current API/worker entrypoints use it.
- internal/factory/factory.go is an older broad Mongo-oriented assembly factory that overlaps current storage/wiring. Do not add new wiring paths there without migrating callers and consolidating composition.
- sdk/go and sdk/go/flowrule are independent modules and both need separate CI coverage. sdk/go/runtime is the original envelope/publisher/consumer helper; sdk/go/flowrule is the newer client SDK with local/HTTP/gRPC/NATS transports and a basic example. Generated consumer code should not depend on inaccessible internal packages from the server module.
- api/*.json defines public JSON schemas. Verify handler validation actually invokes a schema before claiming incoming requests conform to it.

Avoid circular dependencies, driver types in domain/ports, global mutable state, unbounded goroutines or queues, fire-and-forget goroutines, swallowing persistence errors, and using context.Background for request-scoped work.

## 5. Repository file map

### Entrypoints and root files

- README.md — project overview, local start, event flow and current limitations.
- AGENTS.md — this engineering guide and package/file map.
- ARCHITECTURE_MAP.md — runtime topology, data flow, dependency direction and known gaps.
- IMPLEMENTATION_PLAN.md — historical/forward-looking plan; not an authoritative status board unless checkboxes are maintained.
- go.mod / go.sum — root Go module and dependency checksums.
- Makefile — developer build, tests, checks, Docker, migration and codegen targets. Keep target recipes consistent with implemented CLI flags.
- .env.example — configuration names and local-only placeholder defaults. Never place secrets here.
- .gitignore / .dockerignore — build artifacts, generated output, local data, secrets and Docker context exclusions. Do not commit executable binaries.
- .golangci.yml — lint and package-boundary checks.
- SECURITY.md — vulnerability-reporting process and security status.
- Dockerfile.api, Dockerfile.worker, Dockerfile.codegen — multi-stage service/tool images.
- docker-compose.yml — local development dependencies and processes; its example credentials must not be reused for production.
- docker-compose.override.yml — developer override settings.
- .github/workflows/go-ci.yml — build, race-enabled tests and vet for root/nested Go modules.
- .github/instructions/* — focused instructions for Postman collection/folder/request test scripts.
- .opencode/skills.json and .opencode/skills/* — agent skill notes and references; not runtime packages.

### Command packages

- cmd/api/main.go — HTTP API lifecycle and dependency wiring.
- cmd/worker/main.go — worker lifecycle, effect sender selection, NATS setup, scheduler and worker wiring.
- cmd/codegen/main.go — registered-contract code generation CLI.
- cmd/migrate/main.go — migration-up CLI.

### Core business and use-case packages

- internal/config/config.go, config_test.go — environment parsing, raw-value checks, durations and validation.
- internal/domain/types.go — event envelopes, rules, predicates/actions, decisions, execution/effect/workflow/schedule/lease and contract data types.
- internal/domain/errors.go — domain/sentinel errors and error classification.
- internal/domain/batch.go — batch modes/configuration/types.
- internal/ports/broker.go — delivery, broker consumer/publisher, stream/admin, shard-consumer contracts.
- internal/ports/cache.go — cache interface.
- internal/ports/ports.go — transaction manager, transaction-scoped repository set, compiler/evaluator/processor/effect/clock/logger contracts.
- internal/ports/repository.go — inbox, rule, activation, execution, outbox, lease, workflow, schedule, batch, quarantine and contract repository interfaces.
- internal/ports/runtime.go — worker/runtime-specific ports.
- internal/ports/services.go — use-case interfaces consumed by adapters/runtime.
- internal/rules/compiler.go — rule source parsing, validation, limits and immutable compiled revisions.
- internal/rules/compiler_test.go — emit_event action support and composite-predicate validation regressions.
- internal/rules/evaluator.go — predicate/action evaluation and decision construction.
- internal/rules/evaluator_test.go — evaluator behavior tests.
- internal/services/events/service.go — transactional fencing, inbox deduplication, activation/revision lookup, evaluation, execution and outbox writes.
- internal/services/effects/service.go — outbox claiming, delivery, retries/backoff and quarantine coordination.
- internal/services/effects/router.go — broker routing for child events and delegation for external effects.
- internal/services/effects/router_test.go, service_test files — effect-routing and service regression tests.
- internal/services/rules/service.go — revision/activation transactions and rule queries.
- internal/services/batches/service.go, service_test.go — batch formation/processing and query APIs.
- internal/services/workflow/service.go — workflow instance creation/state transitions. Definition helper methods currently contain not-implemented placeholders; HTTP handlers use WorkflowDefinitionRepository separately.
- internal/services/quarantine/service.go — quarantine persistence/query/replay-marking operations.
- internal/application/worker.go — worker runtime orchestration, delivery handling, periodic effect/batch loops and consumer reconciliation.
- internal/runtime/keyqueue/queue.go, queue_test.go — bounded global work, per-key serialization, queue lifecycle and hot-key checks.
- internal/runtime/shard/lease.go — lease acquisition/renewal, ownership cache and fencing tokens.
- internal/runtime/scheduler/scheduler.go — claims due scheduled events and publishes them.
- internal/observability/observability.go — observability lifecycle/configuration.
- internal/observability/logging.go — structured logger.
- internal/observability/metrics.go — metric definitions.
- internal/observability/tracing.go — tracing setup.
- internal/observability/handler.go — HTTP observability handler.
- internal/codegen/generator.go — templates/generation for Go, Java, Protobuf, JSON Schema and runtime artifacts.
- internal/storage/storage.go — backend selection, repository wiring, initialization and closure.
- internal/factory/factory.go — legacy broad factory; current process entrypoints use internal/storage instead.

### Adapters

HTTP:
- internal/adapters/http/router.go — standard-library mux registration and HTTP server lifecycle.
- internal/adapters/http/handlers/rules.go — rule revision/activation/query endpoints.
- internal/adapters/http/handlers/workflows.go — workflow instance and definition endpoints.
- internal/adapters/http/handlers/contracts.go — contract registry and generation endpoints.
- internal/adapters/http/handlers/batch_quarantine.go — batch/quarantine endpoints.
- internal/adapters/http/handlers/scheduled.go — scheduled-event endpoints.
- internal/adapters/http/handlers/utils.go — JSON/YAML request decoder.
- internal/adapters/http/middleware/middleware.go — current tenant extraction and panic recovery; not authentication.
- internal/adapters/http/responses/responses.go — response and error helpers.

Storage/messaging:
- internal/adapters/sql/db.go — PostgreSQL pool, connection settings and migration runner.
- internal/adapters/sql/querier.go — shared query/exec contract for pools and transactions.
- internal/adapters/sql/transaction.go — PostgreSQL transaction manager and transaction object.
- internal/adapters/sql/inbox.go — inbox idempotency.
- internal/adapters/sql/outbox.go — outbox claim/settle/retry and shard lease SQL.
- internal/adapters/sql/rules.go — revisions and activation repository.
- internal/adapters/sql/workflow.go — workflow instances and definitions.
- internal/adapters/sql/batch.go — batch persistence.
- internal/adapters/sql/quarantine.go — quarantine persistence.
- internal/adapters/sql/scheduled.go — scheduled-event persistence.
- internal/adapters/sql/contract.go — contract registry.
- internal/adapters/mongo/db.go, transaction.go — Mongo client/session lifecycle and transactions.
- internal/adapters/mongo/event.go, inbox.go, execution.go, outbox.go, rules.go, shard.go — event processing repository implementations.
- internal/adapters/mongo/workflow.go, batch.go, scheduled.go, quarantine.go, contract.go — workflow and supporting repositories.
- internal/adapters/nats/jetstream.go — JetStream stream configuration, shard consumers, fetch, ACK/NAK/retry, and broker publishing.
- internal/adapters/redis/client.go, cache.go — Redis lifecycle and cache adapter; not in the primary event path today.
- internal/adapters/memory/contract_registry.go — in-memory contract registry.
- internal/adapters/effects/fake.go — FakeDestination, HTTPDestination, fake clock and ID helpers; mixed test/production responsibilities are a candidate for separation.

### Schemas, migrations, SDK and examples

- api/envelope-schema.json — event envelope JSON Schema.
- api/rule-schema.json — rule-set JSON Schema for leaf/composite predicates and emit, emit_event, and command actions. The schema is not automatically applied by every handler; keep it aligned with compiler behavior.
- migrations/001_rule_revisions.sql through migrations/014_workflow_definitions.sql — numbered SQL migrations. There is a numbering gap at 010 and no down-migration set; do not renumber/edit applied migrations.
- sdk/go/go.mod — original independent SDK module definition.
- sdk/go/runtime/envelope.go — public envelope, payload and shard hashing.
- sdk/go/runtime/publisher.go — JetStream publisher.
- sdk/go/runtime/consumer.go — consumer helper.
- sdk/go/runtime/evaluator.go — SDK-side evaluator support.
- sdk/go/flowrule/go.mod — separate client SDK module definition.
- sdk/go/flowrule/client.go, config.go, contract.go — client/config/contract API.
- sdk/go/flowrule/transport*.go — local, HTTP, gRPC, and NATS transport implementations.
- sdk/go/flowrule/shard.go — SDK-compatible shard calculation.
- sdk/go/flowrule/examples/basic/main.go — basic client usage example.
- examples/README.md — example overview and startup guide.
- examples/banking, examples/ecommerce, examples/food-delivery — independent application modules, each with contracts, rules, workflows, backend producer/consumer source, deployment script and generated Go contract types.
- examples/*/contracts — event/command schema definitions.
- examples/*/rules — declarative rule sets.
- examples/*/workflows — workflow state machine definitions.
- examples/*/backend/{producer,consumer}/main.go — sample source. Compiled example binaries are build output and must not be committed.
- examples/*/generated/go — generated source. Regenerate from contract sources/tools rather than hand-editing.

### Tests and docs

- tests/contract/contract_test.go, suite_test.go, mem_repos.go — in-memory repository contract suites and fixtures; not full adapter conformance.
- tests/integration/integration_test.go — use-case tests with mocks/in-memory repositories; not live DB/broker integration.
- docs/README.md — documentation index and API quick start.
- docs/architecture.md — architecture overview and port descriptions.
- docs/design.md — design, data model, consistency and failure-handling notes.
- docs/operations.md — deployment and troubleshooting notes.
- docs/rules.md — rule model/compiler/evaluation guide.
- docs/roadmap.md and docs/future-architecture.md — plans; not proof of shipped functionality.
- docs/01-system-design.mmd through docs/27-order-flow-example.mmd — focused architecture, flow, ordering, schema and operations diagrams.
- docs/audit/* — dated snapshots; re-check every finding against current source before reuse.

## 6. Required engineering rules

- Make one cohesive behavioral change at a time. Prefer explicit constructors and interfaces defined near their consumer.
- Keep .env.example, configuration validation, Dockerfiles, Compose and operational docs consistent.
- Fail closed on security-sensitive configuration; validate numeric values, enum values, timeouts and shard count.
- Use injected clocks for deterministic business logic. Bound all goroutines, queues, outbound calls and shutdown waits.
- For event processing, fencing assertion, dedup, execution/outbox writes and inbox commit state must share the correct transaction. Do not ACK before durable commit.
- Preserve per-key ordering and claimant/fencing semantics. Retry/quarantine/delivery state changes must handle expired and competing claims safely.
- Child event publishing must use the same NATS subject family and NUM_SHARDS as the worker/scheduler/SDK. Event type is in the envelope; do not substitute it for the configured stream subject.
- Bound HTTP request bodies, reject malformed or extra trailing documents, and validate tenant scoping on every resource access.
- Never log secrets or full sensitive payload bodies.
- Do not check in binaries, credentials, local DB data, generated build artifacts or temporary files.
- A bug fix should add a regression test. Concurrency changes require race-enabled tests. Adapter methods need contract coverage for SQL and Mongo before parity is claimed.
- Do not edit applied migrations. Add a new numbered migration and test upgrade behavior. Down migrations must be separately designed/tested before promising rollback.
- Avoid hiding state-changing errors behind logs; an unavailable durable state transition must be observable to the caller/worker.
- Exported Go symbols require useful comments. Keep source formatted and update docs when an interface, route or runtime behavior changes.

## 7. Confirmed risks and backlog

1. P0 security: API has no authN/authZ or trusted tenant resolver. Tenant values from request headers/query/body are not identity. Implement authenticated claims, per-resource tenant authorization and cross-tenant negative tests before untrusted production exposure.
2. P1 outbox ownership: ScheduleRetry and Quarantine do not currently accept claimant identity like MarkDelivered does. Expired/competing publishers can race state changes. Add claimant-fenced transitions.
3. P1 outbox failure handling: publishing is sequential with a fixed claim TTL; some MarkDelivered, ScheduleRetry and quarantine persistence failures are logged and swallowed. Use bounded sends, per-effect timeouts, claimant-safe transitions and return/aggregate durable-state errors.
4. P1 transaction abstraction: ports.Transaction exposes context and fencing mutators; TxRepos combines many repositories; SQL wiring type-asserts concrete transaction types. Redesign around use-case-owned unit-of-work contracts with atomicity tests.
5. P1 adapter conformance: current contract suites use in-memory repositories. Run identical contracts against live PostgreSQL and Mongo transaction-capable configurations; document unsupported semantics.
6. P1 request boundaries: tenant extraction is untrusted and JSON/YAML decoding does not impose a body-size limit or explicitly reject trailing content. Add auth-derived tenant context, request limits and strict decoding.
7. P2 workflow API clarity: workflow.Service definition methods currently return not-implemented errors; handlers use a separate definition repository. Remove or implement the placeholders intentionally.
8. P2 composition: internal/factory overlaps internal/storage and is not used by main entrypoints. Verify callers before deleting or migrating it.
9. P2 operational surface: /health is liveness only; the API has no gRPC listener; migration down/status are unsupported.
10. P2 proof/performance: tests do not prove crash recovery against live PostgreSQL/NATS, lease fencing during long pauses, network failure recovery, real destination idempotency, or throughput/p99 targets. Add failure-injection tests and benchmarks before making those claims.
11. P2 hot path: SQL outbox Insert currently sends one INSERT per effect. Measure and batch inserts/settlement where beneficial.
12. P2 mixed helpers: internal/adapters/effects/fake.go combines production HTTP code with test clock/ID/fake destination; separate by responsibility when changing that area.

## 8. Pull-request evidence checklist

Every PR should state:
- Problem and observable failure mode.
- Packages/files changed and why.
- Data flow and invariants affected.
- Tests run and their actual results (including workflow links/commit SHA).
- Security, data migration, operational and rollback implications.
- Known limitations that remain.

Never report a pending CI check as passed. Never infer live infrastructure coverage from an in-memory test name. Never claim a capability is implemented just because a roadmap, example, diagram or interface mentions it.
