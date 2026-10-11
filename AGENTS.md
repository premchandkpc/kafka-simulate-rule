# AGENTS.md
## FlowRule Engineering Standards

> **Purpose:** A practical guide for engineers and coding agents changing this repository.
>
> **Source of truth:** Current source, configuration, schemas, migrations, tests and CI. Roadmaps, diagrams, comments and old audits are not proof that a feature works. Verify behavior before making claims.

## 1. Non-negotiable principles

1. Inspect the implementation, callers, tests and runtime path before editing.
2. Make one cohesive, observable change at a time.
3. Keep dependencies pointing inward: business policy must not depend on transport, storage or vendor libraries.
4. Reuse existing domain types, schemas, shard-routing logic and helpers. Do not create parallel contracts.
5. Prefer the smallest design that preserves clear ownership, testability and correct failure behavior.
6. Bound concurrency, queues, retries, outbound calls and shutdown waits.
7. Treat errors, cancellation, duplicate delivery, partial failure and recovery as part of the design.
8. Add regression tests for bug fixes. Do not weaken invariants to make tests pass.
9. Distinguish implemented, tested, partially supported, planned and unverified behavior.
10. Never claim a check passed unless its actual result was observed for the commit being reported.

## 2. Product model and event flow

FlowRule is a Go event-driven rules runtime. The root Go module contains the API, worker and CLI tools. SDKs and domain examples have nested Go modules and require separate checks.

### Event lifecycle

1. A producer creates the shared `internal/domain.EventEnvelope` and publishes to the NATS JetStream subject family `events.shard.N`.
2. Shard selection uses the shared tenant/partition-key algorithm and configured shard count. Never implement a competing hash or routing scheme.
3. Workers acquire shard leases and fencing tokens, then consume only the shards they own.
4. A bounded per-key queue preserves ordering for the same partition key while limiting global work.
5. Event processing validates the envelope and fencing token, deduplicates through the inbox, loads the active rule revision, evaluates the rule and writes execution/outbox state in the appropriate transaction.
6. ACK only after successful durable processing. If the durable outcome is unknown, do not ACK as though processing succeeded.
7. The outbox publisher routes emitted child events back to `events.shard.N`; external command effects use the configured destination.
8. The scheduler publishes due events through the same subject family. The existence of batch APIs does not prove that batch worker execution is supported.

### Distributed-systems guarantees

- Delivery is **at-least-once**. Inbox deduplication and transactional outbox semantics protect state transitions; they do not guarantee exactly-once external effects.
- External receivers must honor stable idempotency keys to make repeated effects safe.
- Preserve lease expiry, fencing validation and claimant-token checks. Stale workers/claimants must not settle work owned by a newer claimant.
- Do not claim end-to-end processing, failover, service discovery or durable delivery based only on a producer broker ACK.
- Prove crash recovery and failure behavior with appropriate integration or failure-injection tests.

## 3. Repository map and file ownership

This map explains responsibility; it is not a requirement to make the directory tree symmetrical. Place files where their responsibility belongs.

### Root and operations

| Path | Responsibility |
|---|---|
| `README.md` | Overview, getting started, event flow and limitations |
| `AGENTS.md` | Engineering standards, package ownership and verification workflow |
| `ARCHITECTURE_MAP.md` | Runtime topology, dependencies and known gaps |
| `IMPLEMENTATION_PLAN.md` | Plan/history; verify against source before treating as current |
| `go.mod`, `go.sum` | Root Go module and checksums |
| `Makefile` | Build, test, lint, format, Compose, migrations and code generation |
| `.env.example` | Configuration names and safe placeholders; never secrets |
| `docker-compose.yml`, `docker-compose.override.yml` | Local development stack |
| `Dockerfile.api`, `Dockerfile.worker`, `Dockerfile.codegen` | Core service/tool images |
| `Dockerfile.random-producer` | Opt-in example producer image |
| `.github/workflows/` | CI definitions and verification gates |
| `api/` | Public JSON schemas; verify runtime validation actually uses them |
| `migrations/` | Ordered schema changes; applied migrations are immutable |
| `docs/` | Architecture, operations, diagrams, plans and dated audits |

### Entrypoints and core packages

| Path | Responsibility |
|---|---|
| `cmd/api/main.go` | API composition root, configuration and HTTP lifecycle |
| `cmd/worker/main.go` | Worker composition root, broker setup, services and runtime lifecycle |
| `cmd/codegen/main.go` | Database-backed code-generation CLI |
| `cmd/migrate/main.go` | Migration-up CLI |
| `internal/domain/` | Business types, invariants, validation, IDs, routing helpers and domain errors |
| `internal/rules/` | Pure rule parsing, compilation, validation and evaluation |
| `internal/ports/` | Narrow consumer-owned interfaces/contracts |
| `internal/services/events/` | Fencing, inbox deduplication, rule evaluation, execution and outbox writes |
| `internal/services/rules/` | Rule revision and activation use cases |
| `internal/services/effects/` | Outbox claims, delivery, retries, settlement and quarantine coordination |
| `internal/services/workflow/` | Workflow instance orchestration and state transitions |
| `internal/services/batches/` | Batch use cases; verify worker support separately |
| `internal/services/quarantine/` | Quarantine use cases |
| `internal/application/` | Worker orchestration and lifecycle coordination |
| `internal/runtime/keyqueue/` | Bounded work and per-key serialization |
| `internal/runtime/shard/` | Lease ownership and fencing tokens |
| `internal/runtime/scheduler/` | Scheduled-event claiming and publishing |
| `internal/config/` | Central configuration parsing and validation |
| `internal/storage/` | Storage selection, repository composition and lifecycle |
| `internal/factory/` | Legacy broad factory; current entrypoints use `internal/storage` |
| `internal/codegen/` | Code-generation logic and templates |
| `internal/observability/` | Logging, metrics, tracing and lifecycle |

### Adapters and data boundaries

| Path | Responsibility |
|---|---|
| `internal/adapters/http/` | HTTP routing, handlers, middleware and request/response translation |
| `internal/adapters/nats/` | NATS/JetStream connection, subject mapping, message conversion and ACK/NAK |
| `internal/adapters/postgres/` | SQL repositories, transactions and driver-specific behavior |
| `internal/adapters/mongo/` | Mongo repositories, BSON conversion, indexes and driver-specific behavior |
| `internal/adapters/cache/` | Cache implementation |
| `internal/adapters/effects/` | External effect delivery and development/test destinations |
| `sdk/go/`, `sdk/go/flowrule/` | Separate Go SDK modules |
| `examples/ecommerce/`, `examples/banking/`, `examples/food-delivery/` | Domain-specific contracts, definitions, scripts and tests |
| `examples/random-producer/` | Opt-in producer using existing domain contracts |
| `tests/` | Root test suites; inspect whether each uses mocks, in-memory storage or live services |

Keep vendor types inside adapters. NATS library imports belong in the NATS adapter; services/runtime should depend on transport-neutral ports, not NATS messages, JetStream consumers, SQL rows, Mongo sessions or concrete drivers. Generated consumer code must not import inaccessible server `internal/` packages.

## 4. Dependency direction, SOLID and reuse

Expected direction:

```text
cmd/* (composition roots)
  -> concrete adapters + storage + services/application
services/application/runtime
  -> domain + consumer-owned ports
adapters/storage
  -> ports + domain + external libraries
domain/rules
  -> standard library and justified pure dependencies
```

Review every changed dependency edge:

- **SRP:** one cohesive reason to change per package/type.
- **OCP/LSP:** implementations preserve the documented contract and error semantics.
- **ISP:** consumers depend only on methods they need.
- **DIP:** use cases own policy; adapters own I/O and technology details.
- **Low coupling/high cohesion:** no import cycles, service-to-concrete-adapter shortcuts or hidden global dependencies.
- **Explicit composition:** use existing composition roots; do not add another factory to avoid a small constructor.
- **Meaningful reuse:** compare behavior and invariants before extracting duplicated code. Share common mechanics, not distinct domain policy.
- **Justified interfaces:** interfaces belong near the consumer and should isolate a real boundary or testing seam. Do not add speculative abstractions or catch-all `common`/`utils` packages.

Before adding, moving or deleting a file, identify its single responsibility, owning layer, callers, allowed dependencies, existing alternatives and affected tests/docs/configuration. Trace callers before refactoring; separate policy from mechanism and pure logic from I/O. Remove obsolete code only after checking usage and compatibility. Avoid unrelated mass renames and formatting noise.

## 5. Contracts, persistence and security

For event/API/rule/schema/SDK/configuration changes, assess:

- Source-of-truth schema and whether the runtime actually validates against it.
- Required/optional fields, defaults, unknown-field handling, size limits and malformed-input behavior.
- Backward/forward compatibility with producers, consumers, stored data and generated clients.
- SQL/Mongo parity, indexes, uniqueness, transactions and tenant-scoped queries.
- Migration ordering, repeatability, backfill, downtime/locking and rollback feasibility.
- Tenant propagation, authorization, injection, path traversal, secret leakage and overly broad queries.

Rules:

- Never edit an applied migration to change historical behavior. Add a new numbered migration and test upgrades.
- JSON bytes are not encoded BSON. Store JSON payloads as binary bytes or convert to BSON documents; never cast raw JSON bytes directly to `bson.Raw`.
- Use parameterized SQL and safe BSON construction.
- Bound HTTP request bodies and reject malformed or unexpected trailing documents.
- Enforce tenant scoping at repository query boundaries. Never assign a tenant to orphan data without an authoritative source.
- Fail closed on sensitive production configuration. A trusted-proxy token is not first-party authentication: the proxy must authenticate callers, strip inbound trust headers, and set fresh trusted identity headers.
- Never log secrets, credentials, tokens or full sensitive payloads. Local Compose placeholders must not be reused in production.

Verify these current caveats against source before relying on them:

- `/health` is liveness, not readiness.
- The API starts HTTP only; no gRPC server is implemented here.
- The migration CLI supports `up`; do not advertise down/status.
- Workflow-definition mutation routes are not exposed on the tenant-facing API without authenticated admin controls and an ownership model.
- Batch API presence does not imply worker batch execution is safe or enabled.
- `internal/factory` overlaps `internal/storage`; inspect callers before expanding either.
- Production multi-tenant exposure requires verified authentication, authorization on every resource path, trusted-proxy deployment and cross-tenant negative tests.

## 6. Runtime and Go coding standards

For worker, broker, outbox, scheduler, lease, queue and transaction changes, review at-least-once delivery, duplicates, idempotency, ACK timing, unknown commit outcomes, retry classification, poison messages, quarantine, lease expiry, fencing, per-key ordering, bounded backpressure, tenant propagation, deadlines, cancellation, shutdown and crash recovery.

Go standards:

- Run `gofmt` and follow idiomatic naming/project lint rules.
- Pass `context.Context` explicitly for operation-scoped work. Do not hide request contexts in structs or replace them with `context.Background()` in request paths.
- Wrap errors with useful operation context and preserve classification using `errors.Is`/`errors.As`.
- Do not swallow persistence, cleanup, cancellation or settlement errors.
- Make goroutine ownership, cancellation, synchronization and termination explicit. Avoid detached goroutines and unbounded fan-out.
- Centralize configuration parsing; validate numeric bounds, enums, timeouts, tenant IDs and shard counts at boundaries.
- Use deterministic clocks/IDs where helpful; add table-driven tests, race tests for concurrency changes, and benchmarks before performance claims.
- Document exported symbols and keep public SDK contracts stable.

## 7. Random producer requirements

The producer is a development/load example, not part of API or worker startup.

| File | Responsibility |
|---|---|
| `examples/random-producer/main.go` | Flags/env, signal handling, lifecycle and adapter wiring |
| `examples/random-producer/producer.go` | Bounded workload, narrow `Publisher` contract, publication orchestration and attempted/ACKed/failed counts |
| `examples/random-producer/scenarios.go` | Domain event factories |
| `examples/random-producer/*_test.go` | Producer behavior, scenario rotation and contract validation |
| `Dockerfile.random-producer` | Standalone image used by the opt-in Compose profile |

Requirements:

- Bound clients, messages/client, concurrency and publication work; validate configured ranges.
- Reuse `internal/domain.EventEnvelope` and shared shard-routing logic.
- Keep the use case transport-neutral; NATS connection/publishing belongs in the adapter.
- Support `all`, `ecommerce`, `banking` and `food-delivery`. `all` rotates by a global sequence across clients.
- Validate generated JSON against existing YAML contracts instead of maintaining duplicate schema definitions.
- Keep tenant ID separate from simulated customer/account/client identity.
- Report broker ACKs as broker ACKs only. They do not prove worker processing.
- Keep README, Compose flags, env names, Make targets, Docker context and CI consistent.
- End-to-end claims require correlating generated event IDs with durable execution results and testing worker restart/lease reassignment against real services where feasible.

A typical local workflow, after checking the current README and Compose configuration:

```sh
cp .env.example .env
docker compose up -d postgres nats redis api worker
(cd examples/ecommerce && ./deploy.sh)
(cd examples/banking && ./deploy.sh)
(cd examples/food-delivery && ./deploy.sh)
docker compose --profile examples run --rm random-producer
```

## 8. Build and verification

Run from the repository root unless stated otherwise. Check the current Makefile/CI before assuming a target exists.

### Root module

```sh
go build ./cmd/...
go test -race -count=1 ./...
go vet ./...
```

### Nested Go modules

When their code or shared contracts are affected:

```sh
(cd sdk/go && go test -race -count=1 ./...)
(cd sdk/go/flowrule && go test -race -count=1 ./...)
(cd examples/banking && go test -race -count=1 ./...)
(cd examples/ecommerce && go test -race -count=1 ./...)
(cd examples/food-delivery && go test -race -count=1 ./...)
```

### Useful Make targets

- `make build`: build API, worker and codegen binaries into `bin/`.
- `make test-unit`: root internal unit tests.
- `make test-examples`: random-producer tests, including existing YAML contract checks.
- `make test-integration`: tests under `tests/integration`; inspect them before calling them live infrastructure tests.
- `make test`: combined test target; verify the current Makefile.
- `make fmt` / `make check-fmt`: format or verify Go formatting.
- `make lint` / `make vet`: lint and static analysis.
- `make docker-up` / `make docker-down`: local Compose stack.
- `make migrate-up`: apply pending migrations.
- `make generate`: codegen from a registered contract; requires `CONTRACT`, `CONTRACT_VERSION`, `TARGET` and `OUTPUT_DIR`.

Verification rules:

- Run focused regression tests while developing, then all applicable checks before completion.
- Use live infrastructure tests when claiming SQL/Mongo/NATS behavior. In-memory suites alone do not establish adapter conformance.
- Verify CI against the exact current head SHA. Queued/in-progress is not passed.
- Report exact commands and actual outcomes, skipped checks and remaining risks. Never invent results.

## 9. Required change workflow

### A. Inspect
- Confirm working branch, base branch and current commit.
- Inspect source, callers, tests, schemas, migrations, config, Makefile, Docker/Compose, docs and CI relevant to the change.
- Reproduce the observable issue or state a measurable acceptance criterion.
- Trace success, error, retry, cancellation, shutdown and recovery paths.

### B. Design
For non-trivial or multi-package changes, record:
- Problem and current execution/dependency flow.
- Proposed flow and rationale.
- File/package ownership and interface changes.
- Data/API compatibility, transaction boundaries, idempotency and tenant isolation.
- Failure/security/operational implications and rollback limits.
- Alternatives rejected, acceptance criteria and proof-producing tests.

### C. Implement
- Keep the change cohesive and the dependency direction explicit.
- Update code, tests, schemas, config, docs, examples, Compose and CI together where relevant.
- Remove obsolete code only after checking callers/compatibility.
- Do not include secrets, binaries, temporary files or unrelated churn.

### D. Verify and review
- Format changed files; run focused tests first.
- Run root and relevant nested-module tests, race checks, vet, lint, schema/migration checks, container builds and live tests as applicable.
- Review the complete diff for misplaced files, duplicate logic, unsafe abstractions, stale comments and mismatched docs.
- Inspect CI status for the exact head SHA.

### E. Report
Every completion report must state:
1. What changed and why.
2. Files/packages changed and their responsibilities.
3. The resulting data/dependency flow.
4. Checks actually run and pass/fail/skipped outcomes.
5. CI link and commit SHA when available.
6. Remaining limitations, deployment impact and rollback constraints.

## 10. Definition of done

- [ ] Root cause/requirement addressed with observable behavior.
- [ ] File placement, ownership, dependency direction, SOLID and interface scope reviewed.
- [ ] Duplicate logic assessed; shared behavior extracted only when semantics match.
- [ ] API/event/schema/SDK compatibility and migrations assessed where relevant.
- [ ] Security, tenant isolation, validation, retries, cancellation and failure modes reviewed.
- [ ] Regression tests added; race, contract and live integration tests selected appropriately.
- [ ] Formatting, build, tests, vet, lint and relevant security checks reported accurately.
- [ ] Docker, Compose, env examples, Make targets and CI remain consistent when affected.
- [ ] Documentation and architecture/file maps match implementation.
- [ ] Dead code removed safely; no secrets, generated artifacts or unrelated churn.
- [ ] Remaining risks and unsupported behavior stated honestly.

For documentation-only changes, verify file paths, commands/flags, links, consistency with source, and actionable, non-contradictory instructions. Runtime tests do not validate documentation.
