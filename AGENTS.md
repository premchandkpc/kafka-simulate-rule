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
- Handlers: internal/adapters/http/handlers/rules.go, workflows.go, contracts.go, batch_quarantine.go and scheduled.go. Shared HTTP request helpers live in handlers/utils.go and query_params.go.
- Middleware and request body decoding: internal/adapters/http/middleware/middleware.go and internal/adapters/http/handlers/utils.go.
- Workflow definitions are global platform configuration in the current schema. The tenant-facing API exposes read-only GET routes under /v1/workflows/definitions; create/delete routes are deliberately not registered until an authenticated admin API and explicit ownership model exist.
- /health is liveness only; it does not check storage or broker readiness.
- The HTTP SDK transport targets POST /v1/events/{type}; verify that route is implemented and contract-tested before selecting HTTP transport. Do not assume the management API accepts producer events.
- Scheduled-event list/get/cancel operations use tenant-scoped repository queries. Cancellation is allowed only while the event is pending; repository conflicts return an HTTP conflict rather than reporting success.
- The API starts HTTP only. No gRPC server is implemented; the client SDK's gRPC transport targets a compatible server deployed separately.

SECURITY / DEPLOYMENT REQUIREMENT:
- Development and test may use caller-provided X-Tenant-ID or tenant_id values for local isolation only.
- In non-development environments, TenantExtractor requires FLOWRULE_TRUSTED_PROXY_TOKEN and validates X-Proxy-Auth with a constant-time comparison. It takes tenant identity only from X-Authenticated-Tenant-ID; it does not fall back to caller-controlled tenant headers/query values.
- This is a trusted-proxy integration contract, not first-party authentication. The proxy must authenticate and authorize the caller, strip any inbound X-Proxy-Auth and X-Authenticated-Tenant-ID headers, then set fresh trusted values. Protect the token as a secret and keep API ingress private to the proxy.
- Health is intentionally exempt from tenant identity extraction. It is liveness only, not a readiness/security check.
- Workflow definitions are treated as global, read-only platform configuration at the tenant-facing HTTP boundary; do not re-enable mutations without an authenticated admin API and an explicit ownership model. Quarantine GET/replay/delete operations now carry tenant identity into repository queries, and unscoped list queries fail closed. New effect quarantines carry tenant identity from the event through the outbox; SQL migrations and Mongo startup backfill repair legacy records when their source chain is available. Orphan records without an authoritative source remain unscoped and must not be assigned a tenant from a request. Production multi-tenant exposure remains blocked until proxy deployment, every resource path, and cross-tenant negative tests are verified.

### Worker

- Entry point: cmd/worker/main.go.
- Loads config, initializes observability and storage, constructs the NATS JetStream consumer, event/effect/batch services, lease manager, scheduler, and application.Worker. The batch service is constructed, but non-none BATCH_MODE values are rejected by config because batch execution is not integrated with shard fencing and the per-key queue.
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
- internal/ports declares broker, repository, transaction, runtime and service contracts. Prefer small interfaces owned by the consumer. EventTxRepos and RuleTxRepos are deliberately narrow transaction-scoped views; adapter-owned factories perform concrete transaction assertions and driver-specific repository wiring. The Transaction port still exposes context/fencing details and should not grow without a use-case need.
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
- internal/services/effects/router_test.go and service_test.go — effect-routing, claim-token, and settlement-error regression tests.
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
- internal/adapters/http/handlers/utils.go — JSON/YAML decoder, 1 MiB request-body limit, strict single-document handling.
- internal/adapters/http/handlers/safe_output_path.go and safe_output_path_test.go — code-generation output containment, symlink resolution, and path traversal regression tests.
- internal/adapters/http/handlers/scheduled_test.go, workflows_test.go, batch_quarantine_test.go, quarantine_tenant_test.go — API behavior, tenant-body mismatch, read-only global workflow definitions, and tenant-scoped quarantine regression tests.
- internal/adapters/http/middleware/middleware.go — tenant context establishment and panic recovery. The auth package validates optional/required JWTs; production without a JWT tenant falls back to the fail-closed trusted-proxy contract.
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
- internal/adapters/sql/scheduled.go — scheduled-event persistence, tenant-scoped listing/cancellation, expired-claim recovery, and claimant-fenced settlement.
- internal/adapters/mongo/serialization_test.go — BSON round-trip regression tests for JSON payloads, headers, context, and rule data.
- migrations/015_scheduled_event_claims.sql — claim expiry/claimant/error fields and scheduled-event indexes; never rewrite already-applied migration 011.
- internal/adapters/sql/contract.go — contract registry.
- internal/adapters/mongo/db.go, transaction.go — Mongo client/session lifecycle and transactions.
- internal/adapters/mongo/event.go, inbox.go, execution.go, outbox.go, rules.go, shard.go — event processing repository implementations.
- internal/adapters/mongo/workflow.go, batch.go, scheduled.go, quarantine.go, contract.go — workflow and supporting repositories.
- internal/adapters/mongo/serialization_test.go — BSON round-trip coverage for JSON payloads/context/rule source and compiled rules.
- internal/adapters/nats/jetstream.go — JetStream stream configuration, shard consumers, fetch, ACK/NAK/retry, and broker publishing.
- internal/adapters/redis/client.go, cache.go — Redis lifecycle and cache adapter; not in the primary event path today.
- internal/adapters/memory/contract_registry.go — in-memory contract registry.
- internal/adapters/effects/fake.go — FakeDestination, HTTPDestination, fake clock and ID helpers; mixed test/production responsibilities are a candidate for separation.

### Schemas, migrations, SDK and examples

- api/envelope-schema.json — event envelope JSON Schema.
- api/rule-schema.json — rule-set JSON Schema for leaf/composite predicates and emit, emit_event, and command actions. The schema is not automatically applied by every handler; keep it aligned with compiler behavior.
- migrations/001_rule_revisions.sql through migrations/015_scheduled_event_claims.sql — numbered SQL migrations. There is a numbering gap at 010 and no down-migration set; migration 015 adds scheduled-event claim/expiry/error fields. Do not renumber or edit applied migrations.
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
- Mongo JSON payload/source/context fields must be stored as BSON binary bytes (or properly converted BSON documents), never cast directly from JSON bytes to bson.Raw; bson.Raw expects encoded BSON.
- Batch creation/list/get API handlers may exist while worker batch execution remains unsupported. Do not enable BATCH_MODE until processing receives a real owned-shard fencing token and passes through the key queue.
- Bound HTTP request bodies, reject malformed or extra trailing documents, and validate tenant scoping on every resource access.
- Never log secrets or full sensitive payload bodies.
- Do not check in binaries, credentials, local DB data, generated build artifacts or temporary files.
- A bug fix should add a regression test. Concurrency changes require race-enabled tests. Adapter methods need contract coverage for SQL and Mongo before parity is claimed. BSON JSON-backed document fields use binary byte slices and must retain the round-trip regression coverage.
- Do not edit applied migrations. Add a new numbered migration and test upgrade behavior. Down migrations must be separately designed/tested before promising rollback.
- Avoid hiding state-changing errors behind logs; an unavailable durable state transition must be observable to the caller/worker.
- Exported Go symbols require useful comments. Keep source formatted and update docs when an interface, route or runtime behavior changes.

## 7. Confirmed risks and backlog

1. P0 security: JWT authentication is available when REQUIRE_AUTH=true and JWT_SECRET/issuer/audience are configured; production tenant extraction also supports a fail-closed trusted-proxy fallback. Caller-supplied tenant headers/query/body are never identity when a JWT claim is present. Keep untrusted production exposure blocked until auth is mandatory for the deployment, every resource path is authorized, and cross-tenant negative tests plus proxy configuration are verified.
2. P1 outbox lease duration: delivery/retry/quarantine settlement now checks a unique claim token and claim expiry in SQL and Mongo, and the service returns durable settlement errors. Publishing remains sequential with a fixed two-minute claim TTL and no per-effect timeout, so slow destinations can still outlive the claim.
3. P1 quarantine atomicity: quarantine records and outbox status are stored separately. If cleanup after losing a claim also fails, an orphan quarantine record can remain; live failure-injection tests and a transactional/repairable protocol are still needed.
4. P1 transaction abstraction: event and rule transaction repository bundles are now narrow and adapter-owned factories isolate concrete SQL/Mongo transaction assertions. Remaining debt: ports.Transaction exposes context/fencing mutators; validate their necessity and add transaction atomicity/conformance tests.
5. P1 adapter conformance: current contract suites use in-memory repositories. Run identical contracts against live PostgreSQL and Mongo transaction-capable configurations; document unsupported semantics.
6. P1 tenant authorization: JSON/YAML request bodies are bounded to 1 MiB and extra JSON values/multiple YAML documents are rejected. Workflow definitions are global and read-only on the tenant-facing API; quarantine item reads and mutations now include tenant predicates in repository operations, and unscoped quarantine lists fail closed. Authentication can use configured JWT claims or the fail-closed trusted-proxy deployment contract. JWT tenant claims are authoritative and conflicting caller-supplied tenant values are rejected. Legacy effect quarantine records are backfilled from the outbox/execution source chain where available; orphan records without that source remain unscoped. Verify every resource path and cross-tenant negative tests before production exposure.
7. P2 workflow API clarity: workflow.Service definition methods currently return not-implemented errors; handlers use a separate definition repository. Remove or implement the placeholders intentionally.
8. P2 composition: internal/factory overlaps internal/storage and is not used by main entrypoints. Verify callers before deleting or migrating it.
9. P2 operational surface: /health is liveness only; the API has no gRPC listener; migration down/status are unsupported.
10. P2 proof/performance: tests do not prove crash recovery against live PostgreSQL/NATS, lease fencing during long pauses, network failure recovery, real destination idempotency, or throughput/p99 targets. Add failure-injection tests and benchmarks before making those claims.
11. P1 shard assignment: LeaseManager uses WORKER_REPLICAS (default 1 for local single-worker use) to target ceil(NUM_SHARDS/WORKER_REPLICAS) leases per worker. This is a fixed-replica contract, not dynamic membership/rebalancing: configure the same WORKER_REPLICAS value on every worker and update it deliberately with the deployment. Dynamic membership and coordinated rebalancing remain future work.
12. P2 scheduled retry policy: scheduler publish errors are currently marked failed rather than retried with delayed backoff; define retryable/permanent classes and maximum-attempt behavior.
13. P2 hot path: SQL outbox Insert currently sends one INSERT per effect. Measure and batch inserts/settlement where beneficial.
14. P2 mixed helpers: internal/adapters/effects/fake.go combines production HTTP code with test clock/ID/fake destination; separate by responsibility when changing that area.

## 8. Pull-request evidence checklist

Every PR should state:
- Problem and observable failure mode.
- Packages/files changed and why.
- Data flow and invariants affected.
- Tests run and their actual results (including workflow links/commit SHA).
- Security, data migration, operational and rollback implications.
- Known limitations that remain.

Never report a pending CI check as passed. Never infer live infrastructure coverage from an in-memory test name. Never claim a capability is implemented just because a roadmap, example, diagram or interface mentions it.

## 9. Mandatory change-design workflow

Every non-trivial change MUST follow this workflow before implementation. Do not treat a request as permission to append code wherever it is easiest.

### 9.1 Establish evidence and scope

1. Inspect the current implementation, callers, tests, configuration, schemas, migrations, build targets, and relevant documentation before proposing a design.
2. Identify the current branch and base commit. Keep changes on the requested working branch; never commit directly to the default branch unless explicitly instructed.
3. Write down the observable problem, affected use case, invariants, compatibility constraints, failure modes, and measurable acceptance criteria.
4. Separate verified facts from assumptions. A filename, interface, diagram, or old audit is not proof that a capability works.
5. Trace the full call path and data lifecycle, including startup, normal execution, retries, cancellation, shutdown, and recovery where applicable.

### 9.2 Design before coding

For every change that affects architecture or more than one package, record:

- Current flow and its concrete shortcomings.
- Proposed flow and why it is better.
- Package ownership and the responsibility of each changed/new file.
- Dependency direction before and after the change.
- Interfaces required by consumers, and why each interface belongs to that consumer.
- Data/schema/API compatibility, transaction boundaries, idempotency, tenant isolation, and failure behavior.
- Alternatives considered and the reason for rejecting them.
- Test strategy, rollout/migration steps, operational impact, and rollback limits.

Do not introduce a new layer, interface, factory, registry, generic framework, or package merely for theoretical extensibility. Introduce it when it isolates a real volatile dependency, prevents a real dependency violation, or removes meaningful duplication. Prefer the smallest design that preserves clear ownership and testability.

### 9.3 File and package placement rules

Before adding or moving a file, answer: what single responsibility does it own, which layer owns that responsibility, and which packages should be allowed to depend on it?

- `cmd/*`: process entrypoints and composition roots only. Parse process-level configuration, wire implementations, manage lifecycle, and delegate work. No business rules, repositories, or HTTP handlers implemented in command packages.
- `internal/domain`: business concepts, invariants, state transitions, deterministic business validation, and domain errors. No I/O, serialization-framework dependencies, transport types, or database-driver types.
- `internal/rules`: pure rule parsing/compilation/evaluation and rule limits. No persistence, broker calls, clocks obtained globally, or hidden side effects.
- `internal/ports`: contracts required by inward-facing consumers. Keep interfaces narrow, cohesive, and owned by the package that needs the capability. Avoid speculative interfaces and broad catch-all repository interfaces.
- `internal/services/*`: use-case orchestration and application-level transaction coordination. Depend on domain and ports, not concrete adapters.
- `internal/application`: coordinate worker lifecycle and runtime collaborators; do not duplicate business use cases already owned by services.
- `internal/runtime/*`: reusable runtime mechanics such as bounded key queues, leases, and scheduling. Keep business decisions in the appropriate use case.
- `internal/adapters/*`: protocol, storage, broker, cache, and external-system details; translate at the boundary and implement ports.
- `internal/storage`: backend selection and repository composition. Avoid creating a second competing composition root.
- `internal/observability`: logging, metrics, tracing, and their lifecycle; business packages should not depend on concrete observability vendors.
- `internal/codegen`: code generation logic and templates. HTTP handlers and CLI commands should validate inputs and delegate to it.
- `api/`, `migrations/`, SDKs, examples, and docs: treat schemas, persistence evolution, public client contracts, examples, and architecture documentation as separate owned artifacts with explicit compatibility checks.

These are ownership rules, not a reason to force every file into a prescribed folder. If a responsibility does not fit, explain the boundary and propose the smallest coherent package change. Do not move files just to make the tree look symmetrical.

### 9.4 Dependency and SOLID review gate

For every changed dependency edge, verify:

- Dependency direction points inward; domain and rule evaluation remain independent of infrastructure.
- A package imports only what it uses. No import cycles, adapter-to-adapter shortcuts, or service imports of concrete storage/broker packages.
- Interfaces are small, consumer-driven, and implemented by the boundary adapter. Avoid interfaces with one implementation unless they provide a real boundary or testing seam.
- Single Responsibility: each package/type has one cohesive reason to change.
- Open/Closed and Liskov Substitution: implementations honor the same documented contract and error semantics; do not add abstractions that make behavior implicit.
- Interface Segregation: consumers do not depend on unrelated repository methods.
- Dependency Inversion: use cases own policy; adapters own technical details.
- Composition is explicit and testable. Do not add another factory/wiring route when an existing composition root can be improved.
- Shared helpers have stable semantics and a clear owner. Avoid both copy-pasted policy and a miscellaneous `common`/ `utils` package that becomes a dependency dumping ground.

When logic is duplicated, compare behavior and invariants before extracting it. Share genuinely identical mechanics; keep domain-specific policies separate. Add tests for shared behavior and regression tests for each affected caller.

### 9.5 Structural refactoring requirements

When a change reveals misplaced or tangled code:

1. Map callers and side effects before moving code.
2. Move code to the package that owns the responsibility; update imports and composition wiring.
3. Separate policy from mechanism and pure logic from I/O.
4. Remove obsolete implementations, dead code, stale comments, duplicate factories, and superseded helpers only after confirming there are no callers or public compatibility requirements.
5. Keep changes cohesive. Avoid unrelated mass renames or formatting noise that obscures behavioral changes.
6. Update tests, diagrams, file maps, and README/operational instructions affected by the new structure.
7. Explain intentional exceptions rather than silently violating a boundary.

Do not preserve a bad design solely to minimize the diff. Equally, do not rewrite stable code without a demonstrated correctness, maintainability, security, or performance benefit.

### 9.6 Data contracts, schemas, and migrations

Any change to an event envelope, rule model, API payload, persisted record, SDK contract, generated artifact, or configuration format must assess:

- Source-of-truth schema and actual runtime validation.
- Required/optional fields, defaults, unknown-field handling, size limits, and malformed-input behavior.
- Backward/forward compatibility with deployed producers, consumers, and stored records.
- SQL and Mongo parity, indexes, uniqueness, transaction semantics, and tenant-scoped query predicates.
- Migration ordering, repeatability, existing-data backfill, lock/downtime implications, and rollback feasibility.
- Generated code and SDK examples, plus contract tests.
- Security implications such as tenant spoofing, injection, path traversal, secret leakage, and over-broad queries.

Never rewrite an already-applied migration to change historical behavior. Add a new migration. Do not claim schema enforcement merely because a schema file exists; prove that the request or message path invokes validation.

### 9.7 Runtime and distributed-systems invariants

Changes to worker, broker, outbox, scheduler, lease, queue, or transaction behavior must explicitly review:

- At-least-once delivery, duplicate delivery, stable idempotency keys, and inbox/outbox atomicity.
- ACK timing, unknown commit outcomes, retry classification, backoff, quarantine, and poison-message handling.
- Lease expiry, fencing-token validation, stale workers, claim expiry, and claimant-fenced settlement.
- Per-key ordering, bounded queues, backpressure, goroutine lifecycle, cancellation, and graceful shutdown.
- Tenant propagation and authorization at repository query boundaries.
- Timeout/deadline behavior, error propagation, observability, and recovery after process or broker failure.

Do not weaken an invariant to make a test pass. Add failure-injection tests for meaningful crash windows. Do not claim exactly-once behavior without a precise, bounded definition and evidence.

### 9.8 Go engineering standards

- Follow `gofmt`, project lint rules, idiomatic naming, and effective Go error handling.
- Pass `context.Context` explicitly for request/operation-scoped work; do not hide it in structs or replace it with `context.Background()` inside a request path.
- Wrap errors with useful operation context and preserve classification via `errors.Is`/`errors.As` where needed. Never swallow persistence or cleanup errors silently.
- Make goroutine ownership, cancellation, termination, and synchronization explicit. Avoid unbounded fan-out, detached goroutines, and shared mutable state without synchronization.
- Validate untrusted inputs at boundaries; use parameterized SQL, safe BSON construction, bounded request bodies, and explicit outbound destination/time limits.
- Keep configuration parsing and validation centralized. Fail fast for invalid or unsafe production configuration.
- Add table-driven tests where useful, deterministic clocks/IDs for time-dependent logic, race tests for concurrency, and benchmarks before claiming performance improvements.
- Keep exported APIs documented and stable. Do not expose internal server packages through generated SDK code.

### 9.9 Verification and evidence

Run the narrowest relevant tests during development, then run all applicable checks before declaring completion:

```sh
gofmt -w <changed-go-files>
go build ./cmd/...
go test -race -count=1 ./...
go vet ./...
```

Also run the separate SDK/example module checks listed in Section 2 when their code or shared contracts are affected, plus relevant lint, schema, migration, security, and integration checks. Use live infrastructure tests when claiming SQL/Mongo/NATS behavior; mocks and in-memory repositories alone do not establish adapter conformance.

Report exact commands and outcomes. If a check cannot run, say why and identify the remaining risk. Never report an unobserved CI result as passed.

### 9.10 Definition of done for every non-trivial PR

A change is not complete until the PR description or review record includes:

- [ ] Root cause and observable behavior addressed.
- [ ] Before/after execution or dependency flow.
- [ ] File placement and ownership justified for every new/moved file.
- [ ] SOLID, coupling, interface segregation, duplication, and abstraction trade-offs reviewed.
- [ ] API/schema/migration/SDK compatibility assessed where relevant.
- [ ] Failure modes, security, tenancy, retries, and operational behavior assessed where relevant.
- [ ] Unit/regression tests added; contract, race, and live integration tests selected appropriately.
- [ ] Formatting, build, tests, vet, and relevant lint/security checks reported with actual results.
- [ ] Architecture map, file map, schema docs, README, and operational docs updated when affected.
- [ ] Dead code and obsolete paths removed where safe; no unrelated changes or secrets introduced.
- [ ] Remaining limitations and rollout/rollback constraints stated honestly.

For a documentation-only change, do not pretend runtime tests validate the document. Check links, commands, path names, consistency with current source, and whether requirements are actionable and non-contradictory.

