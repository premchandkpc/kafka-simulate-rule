# AGENTS.md — FlowRule Engineering Standards

> **Purpose:** Give engineers and coding agents one reliable set of rules for understanding, changing, testing, and reviewing this repository.
>
> **Priority:** Current source, executable tests, configuration, migrations, and CI are the evidence of what the system does. This guide describes intended boundaries and required practices; it is not proof that a feature works.

## 1. Working rules

1. Inspect before editing. Read the relevant implementation, callers, tests, configuration, schemas, migrations, build targets, and documentation.
2. Trace the complete behavior. Follow data from entry point through validation, use case, persistence or broker interaction, and the resulting state.
3. Make cohesive changes. Fix the cause, not just the visible symptom. Avoid unrelated rewrites and cosmetic file moves.
4. Preserve existing contracts unless a change explicitly requires a migration. Consider API, event, database, SDK, and configuration compatibility.
5. Keep dependencies pointed inward. Business policy belongs in domain/use-case code; infrastructure details belong at adapters and composition roots.
6. Prefer simple, explicit designs. Add interfaces or layers only when they protect a real boundary, enable meaningful substitution, or remove genuine duplication.
7. Prove claims. Report the exact checks run and their actual outcomes. A queued CI run is not a pass; a mock test is not proof of live infrastructure behavior.
8. Never commit secrets, credentials, local database data, binaries, temporary files, or generated build artifacts.

## 2. System overview and event lifecycle

FlowRule is a Go event-driven rules runtime. The root Go module contains the API, worker, and command-line tools. SDKs and domain examples may contain nested Go modules and must be tested separately.

### 2.1 Main event path

1. A producer creates a valid \`domain.EventEnvelope\` and publishes it to the \`events.shard.N\` subject family. Shard selection must use the shared domain routing logic and the same configured shard count as the worker.
2. The worker discovers and claims shards through the lease mechanism. Ownership and fencing tokens protect against stale workers.
3. The worker consumes deliveries and passes them through bounded runtime queues. Events with the same partition key must retain their required ordering.
4. Event processing validates the envelope and ownership token, deduplicates through the inbox, resolves the active rule revision, evaluates the rule, and persists execution and outbox effects in the appropriate transaction.
5. The broker delivery is acknowledged only after the durable processing outcome is known. Follow the configured retry or quarantine path when processing fails; do not acknowledge an unknown outcome.
6. The effects publisher claims outbox work and routes emitted child events back through \`events.shard.N\`. Non-event effects use the configured external destination.
7. The scheduler publishes due scheduled events. Batch APIs and batch execution are separate capabilities; do not enable unsupported runtime modes by assumption.

### 2.2 Delivery and correctness model

- Treat broker delivery as **at least once**. Consumers must tolerate duplicates.
- Inbox deduplication and transactional outbox writes protect internal state transitions when implemented and used in the same transaction.
- External side effects are only effectively-once when the receiver honors a stable idempotency key.
- A broker publish acknowledgement proves broker acceptance, **not** worker consumption, durable execution, or completion of downstream effects.
- Never claim exactly-once delivery, failover correctness, or end-to-end completion without a defined guarantee and tests that demonstrate it.

## 3. Architecture and package ownership

Keep business rules independent of transport, storage, and deployment details. Entrypoints compose concrete implementations; adapters translate external protocols into internal contracts.

| Location | Owns | Must not become |
| --- | --- | --- |
| \`cmd/*\` | Process entrypoints, configuration loading, dependency wiring, signal handling, startup and shutdown | Business logic, repositories, or HTTP handler implementations |
| \`internal/domain\` | Business types, invariants, validation, deterministic IDs/hashes, state transitions, domain errors | I/O, transport types, database-driver types, or adapter dependencies |
| \`internal/rules\` | Rule parsing, compilation, limits, deterministic evaluation | Persistence, broker calls, global clocks, randomness, or hidden side effects |
| \`internal/ports\` | Narrow contracts needed by inward-facing consumers | Third-party transport types, catch-all interfaces, or speculative abstractions |
| \`internal/services/*\` | Use-case orchestration and transaction coordination | Direct dependencies on concrete SQL, MongoDB, NATS, or HTTP adapters |
| \`internal/application\` | Worker lifecycle and coordination of runtime collaborators | Duplicate business use cases already owned by services |
| \`internal/runtime/*\` | Runtime mechanics such as bounded queues, shard leases, and scheduling | Unrelated domain policy or unbounded background work |
| \`internal/adapters/*\` | HTTP, broker, persistence, cache, and external-system integration | Business policy that belongs inward |
| \`internal/storage\` | Backend selection, repository construction, initialization, and closure | A second competing composition root |
| \`internal/observability\` | Logging, metrics, tracing, and lifecycle integration | Business decisions or vendor-specific dependencies in domain code |
| \`internal/codegen\` | Artifact generation and templates | HTTP routing or CLI lifecycle management |
| \`api/\`, migrations, SDKs, examples, docs | Public schemas, persistence evolution, client contracts, demonstrations, and engineering documentation | Unreviewed changes that silently diverge from runtime behavior |

These are responsibility boundaries, not a demand to force every file into a perfectly symmetrical tree. Before adding or moving a file, identify its single responsibility, owning layer, callers, and allowed dependencies. Move files only when ownership or dependency direction materially improves.

### 3.1 Composition and interfaces

- Treat \`cmd/api\` and \`cmd/worker\` as composition roots. Construct concrete adapters there or through the established storage/wiring path, then inject them into use cases.
- Define interfaces near the consumer that needs the behavior. Keep them small, cohesive, and explicit about error and lifecycle semantics.
- Avoid an interface with one implementation when it creates no meaningful boundary or test seam. Do not create generic registries, factories, service locators, or frameworks for hypothetical future use.
- Keep transaction-scoped repository contracts narrow. Driver-specific transaction assertions and repository wiring belong in adapter-owned implementations.
- \`internal/factory\` is a legacy broad assembly path that overlaps \`internal/storage\`. Inspect callers before changing or deleting it; do not add another wiring path by default.
- Share stable mechanics only when behavior and invariants are truly identical. Do not move unrelated helpers into a miscellaneous \`common\` or \`utils\` package.

### 3.2 NATS and transport boundary

- The NATS Go client and JetStream-specific types belong inside \`internal/adapters/nats\`.
- Application services, runtime packages, domain types, and ports must not expose NATS connection, message, fetch, ACK/NAK, stream, or subject-mapping types.
- Use transport-neutral publisher/consumer and delivery contracts at internal boundaries.
- The generic FlowRule event stream uses \`events.shard.N\`; the event type belongs in the envelope. Do not route the generic pipeline through an example-specific subject or contract without an explicit domain requirement.
- Keep shard hashing, tenant/partition-key routing, and shard-count assumptions consistent across producers, worker, scheduler, and SDK integrations.

## 4. Important entrypoints and file map

This is a responsibility guide, not an exhaustive inventory. Confirm current paths in the repository before editing; update this section when a structural change makes it inaccurate.

### Root and operations

- \`README.md\`: project purpose, getting started, event flow, and user-facing limitations.
- \`AGENTS.md\`: engineering rules and agent workflow.
- \`ARCHITECTURE_MAP.md\`: architecture, runtime topology, dependency flow, and known gaps.
- \`Makefile\`: canonical developer commands; keep targets aligned with actual CLI flags and module boundaries.
- \`.env.example\`: non-secret local configuration examples. Never put real credentials here.
- \`docker-compose.yml\` and override files: local service composition; development credentials are not production credentials.
- \`Dockerfile.api\`, \`Dockerfile.worker\`, \`Dockerfile.codegen\`: service/tool images.
- \`Dockerfile.random-producer\`: opt-in example producer image.
- \`.github/workflows/go-ci.yml\`: automated build, test, lint/analysis, and image checks. Inspect its current jobs rather than assuming the list here is exhaustive.
- \`SECURITY.md\`: vulnerability reporting and security process.
- \`IMPLEMENTATION_PLAN.md\`, roadmap/future-architecture docs, and dated audit files: historical or forward-looking context. Verify every claimed status against current code.

### Commands

- \`cmd/api/main.go\`: API lifecycle and dependency wiring.
- \`cmd/worker/main.go\`: worker lifecycle, broker setup, runtime/service wiring, and shutdown.
- \`cmd/codegen/main.go\`: database-backed registered-contract generation CLI.
- \`cmd/migrate/main.go\`: migration CLI. Advertise only commands actually implemented.

Command entrypoints should delegate to internal packages. Resource-owning \`run\` functions should return errors so deferred cleanup can run; avoid \`log.Fatal\`, \`log.Fatalf\`, or \`os.Exit\` inside those functions.

### Core packages

- \`internal/config\`: environment parsing, raw-value checks, defaults, and validation.
- \`internal/domain\`: event envelope, rules/actions, decisions, executions/effects, schedules, leases, contracts, and domain errors.
- \`internal/ports\`: repository, transaction, broker, cache, runtime, and service contracts.
- \`internal/rules\`: rule compiler and evaluator.
- \`internal/services/events\`: event-processing use case and durable state transition.
- \`internal/services/effects\`: outbox claiming, dispatch, retry/settlement, and quarantine coordination.
- \`internal/services/effects/router.go\`: routes child events to the broker and non-event effects to the external sender.
- \`internal/services/rules\`: rule revision and activation use cases.
- \`internal/services/batches\`: batch use cases and query operations.
- \`internal/services/workflow\`: workflow-instance operations. Verify any definition-management methods before assuming they are implemented.
- \`internal/services/quarantine\`: quarantine queries and replay-related operations.
- \`internal/application/worker.go\`: worker coordination, delivery handling, and periodic loops.
- \`internal/runtime/keyqueue\`: bounded work and per-key serialization.
- \`internal/runtime/shard\`: shard lease ownership, renewal, and fencing.
- \`internal/runtime/scheduler\`: scheduled-event claims and publication.
- \`internal/storage\`: backend selection and repository composition.
- \`internal/observability\`: logging, metrics, tracing, and instrumentation lifecycle.
- \`internal/codegen\`: code generation logic and templates.
- \`internal/factory\`: legacy factory; inspect current callers before extending it.

### Adapters and external contracts

- \`internal/adapters/http/router.go\`: HTTP server lifecycle and route registration.
- \`internal/adapters/http/handlers/*\`: request/response handling, boundary validation, and mapping use-case results to HTTP.
- \`internal/adapters/nats/jetstream.go\`: NATS/JetStream implementation of internal broker contracts.
- SQL and MongoDB adapters under \`internal/adapters\`: driver-specific persistence, queries, migrations or index setup, and transaction behavior.
- \`api/*.json\`: public JSON schema artifacts. A schema file alone does not prove that an API path invokes schema validation.
- \`sdk/go\`, \`sdk/go/flowrule\`: separate Go modules and public client contracts. Test each module independently.
- \`examples/ecommerce\`, \`examples/banking\`, \`examples/food-delivery\`: domain contracts and example deployments.
- \`examples/random-producer\`: opt-in bounded load generator that reuses the domain envelope, shard routing, and existing e-commerce/banking/food-delivery contracts. Its scenario tests should validate generated payloads against the corresponding YAML contracts. It is not part of API or worker startup.

## 5. Security, tenancy, and configuration

Security behavior must be established from current middleware, configuration, routes, and repository predicates. Do not infer production safety from a flag or a passing unit test.

- Fail closed for missing or invalid production security configuration.
- In non-development deployments, tenant identity must come from the configured authenticated identity mechanism, not from arbitrary caller-controlled tenant headers, query parameters, or body fields.
- For the trusted-proxy contract, the proxy must authenticate/authorize the caller, strip inbound trusted-identity/authentication headers, set fresh trusted values, and be the only path to the private API ingress. Protect the proxy token as a secret. A proxy token is not itself end-user authentication.
- If JWT authentication is configured, verify issuer, audience, signature, required scopes, and authoritative tenant claims. Reject conflicting untrusted tenant values.
- Apply tenant predicates at the repository query boundary for every tenant-owned read and mutation, including list, replay, cancel, and delete paths. Unscoped tenant-facing queries must fail closed.
- Treat platform-global resources separately from tenant-owned resources. Do not expose global mutation endpoints without explicit authenticated admin authorization and an ownership model.
- Bound request bodies, reject malformed or extra trailing JSON/YAML documents, validate path and query parameters, use parameterized SQL, construct BSON safely, and constrain generated output paths.
- Never log secrets, authentication tokens, or full sensitive payloads.
- Keep health/liveness and readiness distinct. Do not claim a liveness endpoint verifies storage, broker, or dependency readiness unless it actually does.
- Keep \`.env.example\`, config validation, Compose, Dockerfiles, deployment docs, and tests consistent. Do not enable a runtime mode merely because its configuration field exists.

## 6. Data, persistence, and compatibility rules

For changes to event envelopes, rule models, API payloads, stored records, SDKs, generated code, or configuration formats:

1. Find the authoritative schema and trace where validation is actually invoked.
2. Define required and optional fields, defaults, unknown-field behavior, limits, and malformed-input behavior.
3. Check compatibility with deployed producers/consumers, existing stored records, generated clients, and examples.
4. Verify SQL and MongoDB behavior separately: indexes, uniqueness, query scoping, atomicity, transaction support, and error semantics.
5. Assess migration ordering, repeatability, backfills, locking/downtime, and rollback feasibility.
6. Add or update contract tests and examples for the affected behavior.

Additional rules:

- Never rewrite an already-applied migration to change historical behavior. Add a new numbered migration and test upgrade behavior.
- Do not promise rollback unless a tested down-migration or recovery procedure exists.
- Use parameterized SQL. For MongoDB, store JSON bytes as BSON binary or explicitly convert them to BSON documents; do not cast raw JSON bytes to \`bson.Raw\`, which represents encoded BSON.
- Preserve tenant ownership and source-of-truth relationships when migrating legacy records. Never infer ownership from a request when the authoritative source is missing.
- Generated code must not depend on inaccessible \`internal\` packages from the server module.
- Keep API/schema documentation synchronized with real runtime validation and behavior.

## 7. Runtime, concurrency, and distributed-systems invariants

Changes to event processing, broker handling, queues, leases, outbox, scheduler, or transactions must explicitly consider:

- **Durability and ACK timing:** do not ACK before the required durable outcome is known.
- **Idempotency:** handle redelivery and duplicate requests with stable keys and clear uniqueness boundaries.
- **Transactions:** keep fencing checks, inbox/dedup state, execution records, and outbox writes within the intended atomic boundary.
- **Fencing and leases:** reject stale tokens; handle expiry, renewal failure, stale workers, and competing claims safely.
- **Claim settlement:** settlement must be fenced by the current claim identity/expiry where required; a stale claimant must not overwrite a newer result.
- **Ordering and backpressure:** preserve per-key ordering where required; bound queues, worker concurrency, fetch batches, and outbound requests.
- **Lifecycle:** every goroutine needs clear ownership, cancellation, termination, and synchronization. Shutdown must be bounded and must not abandon required durable work silently.
- **Retries and poison messages:** distinguish retryable and permanent failures, bound attempts/backoff, and make quarantine outcomes observable.
- **Failure windows:** consider crashes before and after commit, publish, ACK, settlement, and lease transfer. Add failure-injection tests for material risks.
- **Observability:** propagate useful operation context in errors and expose enough metrics/logging to diagnose retries, lag, ownership, and failures without leaking secrets.

Never weaken a correctness invariant merely to make a test pass. Do not claim that broker ACK means consumer completion, that replica configuration provides dynamic membership, or that a happy-path test proves failover recovery.

## 8. Go implementation standards

- Run \`gofmt\` on changed Go files and follow repository lint rules and idiomatic naming.
- Pass \`context.Context\` explicitly for request- or operation-scoped work. Do not hide it in structs or replace it with \`context.Background()\` in request paths.
- Wrap errors with useful operation context. Preserve error classification using \`errors.Is\` and \`errors.As\` where appropriate.
- Never silently swallow persistence, cleanup, cancellation, or settlement errors.
- Keep concurrency bounded. Avoid detached goroutines, unbounded fan-out, unsafe shared mutable state, and goroutine leaks.
- Inject clocks, IDs, and randomness when deterministic tests or reproducibility require them.
- Validate untrusted inputs at boundaries; use explicit timeout/deadline policies for network and external-destination calls.
- Document exported symbols and keep public contracts stable unless a breaking change is intentional and documented.
- Add regression tests for bug fixes, race-enabled tests for concurrency changes, and benchmarks before making performance claims.
- Avoid duplicated policy. Extract shared code only when semantics match and there is a clear owner.

## 9. Development and verification workflow

### 9.1 Before implementation

For any non-trivial or cross-package change:

1. Identify the branch and base commit. Work on the requested branch; do not commit directly to the default branch unless explicitly instructed.
2. Reproduce or describe the observable failure with evidence.
3. Trace callers, state changes, external side effects, cancellation, retries, and shutdown.
4. Record the current flow, the proposed flow, the affected file/package ownership, and the dependency changes.
5. Define acceptance criteria and tests, including failure cases and compatibility requirements.
6. Consider simpler alternatives. Do not add a layer or abstraction without a concrete benefit.

### 9.2 During implementation

1. Keep the diff cohesive and reviewable.
2. Update configuration, schemas, migrations, tests, Compose, build targets, and docs together when affected.
3. Remove obsolete code only after checking callers and compatibility obligations.
4. Do not preserve a bad boundary just to minimize the diff; do not rewrite stable code without a demonstrated correctness, maintainability, security, or performance benefit.
5. Re-check the complete flow after moving code or changing wiring.

### 9.3 Root-module checks

Run the relevant narrow test while iterating. Before completion, run the applicable full checks:

~~~sh
gofmt -w <changed-go-files>
go build ./cmd/...
go test -race -count=1 ./...
go vet ./...
~~~

Useful project targets include:

- \`make build\`: build configured binaries.
- \`make test-unit\`: unit tests for internal packages.
- \`make test-examples\`: random-producer tests, including domain contract checks.
- \`make test-integration\`: tests under \`tests/integration\`; inspect their implementation before describing their coverage.
- \`make fmt\` / \`make check-fmt\`: format and verify formatting.
- \`make lint\` / \`make vet\`: lint and static analysis.
- \`make docker-up\` / \`make docker-down\`: manage the local Compose stack.
- \`make migrate-up\`: apply pending SQL migrations.
- \`make generate\`: code generation; inspect current required variables and CLI flags first.

### 9.4 Nested Go modules and infrastructure

Nested modules do not automatically receive coverage from root-module commands. When affected, run checks separately:

~~~sh
(cd sdk/go && go test -race -count=1 ./...)
(cd sdk/go/flowrule && go test -race -count=1 ./...)
(cd examples/banking && go test -race -count=1 ./...)
(cd examples/ecommerce && go test -race -count=1 ./...)
(cd examples/food-delivery && go test -race -count=1 ./...)
~~~

Use live PostgreSQL, MongoDB, and NATS tests when making claims about their transaction, persistence, delivery, or recovery behavior. In-memory repositories and mocks are useful but do not establish adapter conformance. For end-to-end producer claims, correlate generated event IDs with durable worker results; publish ACK counts alone are insufficient.

### 9.5 Reporting results

- List the exact commands run and whether each passed, failed, or could not run.
- Include the relevant CI workflow/run and commit SHA when available.
- State environmental limitations and unverified risks.
- Never report queued or running checks as passed.
- Never infer support from a type, interface, route name, configuration field, diagram, roadmap, or example alone. Verify the path is wired and exercised.

## 10. Pull-request definition of done

For each non-trivial change, the PR description or review record must cover:

- [ ] Problem and observable behavior addressed.
- [ ] Before/after flow and affected invariants explained.
- [ ] New/moved files have clear ownership and appropriate placement.
- [ ] Coupling, dependency direction, SOLID, interface segregation, and duplication reviewed.
- [ ] Public contracts, schemas, persistence, SDKs, and migration compatibility assessed where relevant.
- [ ] Security, tenant isolation, retries, concurrency, and operational behavior assessed where relevant.
- [ ] Regression and appropriate contract/race/failure-injection tests added.
- [ ] Formatting, build, tests, vet, lint/security checks, and CI results reported accurately.
- [ ] README, architecture map, file map, schema docs, and operational docs updated where affected.
- [ ] No secrets, binaries, dead paths, or unrelated changes introduced.
- [ ] Remaining limitations, deployment prerequisites, and rollback constraints stated honestly.

For documentation-only changes, validate paths, commands, links, terminology, and consistency with the current source. Do not claim runtime behavior was tested by formatting a Markdown file.

## 11. Known limitations and status discipline

Do not maintain a long list of time-sensitive bug claims here unless each item has a clear owner and verification date. Track active defects in issues or a maintained implementation plan. When investigating an older audit or plan:

1. Reproduce the reported behavior against the current branch.
2. Inspect the current implementation and regression tests.
3. Mark the finding as confirmed, fixed, not reproducible, or unverified.
4. Avoid carrying stale findings forward as current facts.

At minimum, verify these areas before making strong production-readiness claims:

- Production authentication, authorization, tenant isolation, and trusted-proxy configuration.
- SQL/MongoDB adapter conformance and transaction semantics.
- Durable event processing, outbox settlement, retry/quarantine behavior, and crash recovery.
- Lease fencing, replica coordination, worker restart, and shard reassignment.
- Readiness checks, bounded shutdown, operational metrics, and real workload performance.
- End-to-end consumption and downstream completion for the random-producer example.

**Final rule:** describe what the code and tests prove, not what the architecture intends to do.
