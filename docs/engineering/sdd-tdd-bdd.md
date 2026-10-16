# SDD, TDD, BDD, and E2E for FlowRule

This guide defines a lightweight workflow from a behavior request to verified behavior in FlowRule. Use the amount of process that matches the risk: a typo does not need a mini design review, but a change to tenant isolation, transactions, retries, leases, or message acknowledgement does.

## The practices fit together

- **Spec-Driven Development (SDD):** agree on the problem, contract, constraints, invariants, failure behavior, and acceptance criteria before implementation.
- **Behavior-Driven Development (BDD):** express the important externally observable outcomes as reviewable Given/When/Then scenarios.
- **Test-Driven Development (TDD):** for each behavior, write a focused failing test, implement the smallest correct change, then refactor while keeping tests green.
- **End-to-end (E2E) testing:** verify that the real application path and its important dependencies work together. E2E tests complement, rather than replace, fast unit tests.

The intended chain is: **spec → acceptance scenarios → failing test → implementation → integration/E2E verification → review**. A Markdown scenario is not an executable test until a test actually asserts its outcome.

## Repository-specific workflow

1. **Inspect before editing.** Read AGENTS.md, the implementation, callers, tests, contracts/migrations, configuration, Makefile, Compose files, and CI relevant to the change. Treat current source and test results as evidence; historical plans are not proof of behavior.
2. **Write the smallest useful spec.** Record the problem, goals/non-goals, affected interfaces, tenant/security boundaries, invariants, failure/retry semantics, observability, compatibility, and acceptance criteria. Use the feature template in this directory.
3. **Add BDD acceptance scenarios.** Include the success path plus the highest-risk negative and recovery paths. Link each scenario to an existing test, a new test, or an explicit gap.
4. **Use TDD at the right layer.** Test pure decision logic at unit level. Test service orchestration with fakes when that is the intended boundary. Do not use mocks to claim that SQL transaction isolation, JetStream redelivery, or lease fencing works.
5. **Test the real boundary.** Use adapter/contract tests for repository behavior and the tagged E2E harness for PostgreSQL + NATS JetStream behavior. Add deterministic failure injection where possible; avoid relying on arbitrary sleeps for correctness.
6. **Run the applicable gates.** Check the exact commands in .github/workflows/go-ci.yml and the Makefile. Nested Go modules need their own test commands. E2E tests use Testcontainers and require a working Docker runtime.
7. **Report evidence honestly.** Distinguish passed, failed, skipped, not run, and not yet implemented. A green test command can still contain skipped scenarios. Never report an unexecuted local check as passing.
8. **Review the guarantee, not just the diff.** Confirm code, tests, docs, schemas, migration behavior, and operations agree. State remaining limitations in the PR.

## FlowRule invariants to consider

Apply the invariants relevant to the change, and define their exact scope in the spec.

- **Tenant isolation:** tenant identity is established at a trusted boundary and every tenant-scoped read/write is scoped at the storage boundary. A caller-supplied development header is not production authentication.
- **Idempotent event processing:** duplicate delivery must not create duplicate logical executions. Verify uniqueness and transaction behavior, not only a mock's call count.
- **Atomic durable state:** inbox state, execution state, and resulting outbox effects must commit atomically where the design requires it.
- **ACK ordering:** acknowledge a JetStream message only after the durable processing outcome is committed. An uncertain commit outcome must not be treated as success.
- **Retry and ordering:** document whether retries block later events for the same key, how permanent failures are classified, and what happens on retry exhaustion.
- **Lease fencing:** stale workers must not commit after ownership has moved to a newer fencing token.
- **Rule revision consistency:** each execution must use a defined active immutable revision; do not infer this guarantee from cache behavior alone.
- **Outbox delivery:** external delivery is at-least-once. Stable idempotency keys help only when the destination honors them. Do not claim exactly-once execution of arbitrary external effects.
- **Operational visibility:** retries, quarantine, lease loss, and delivery failure should be diagnosable without logging secrets or sensitive tenant payloads.

These are properties to verify against implementation and deployment conditions, not blanket claims that every path already satisfies them.

## Test layers and what they prove

| Layer | Use it for | What it does not prove by itself |
| --- | --- | --- |
| Unit | Pure rules, validation, retry classification, deterministic state transitions | Real database, broker, or process-crash behavior |
| Service/component | Use-case orchestration with fake ports | Adapter correctness or SQL isolation |
| Adapter/contract | Repository and adapter contracts; tenant filtering; mapping | Full worker-to-broker lifecycle unless that path is exercised |
| Integration | Several components wired together; inspect whether dependencies are real or mocked | Real infrastructure semantics when in-memory fakes are used |
| E2E | Worker + PostgreSQL + NATS JetStream path using the tagged Testcontainers harness | Scenarios that are skipped or not asserted by the harness |

## Commands used by this repository

Run from the repository root. Use the same commands in CI when comparing results.

- Focused unit tests: **go test -race -count=1 ./internal/...**
- Mock-based integration/service suite: **go test -race -tags=integration -count=1 ./tests/integration/...**
- PostgreSQL + NATS E2E suite: **go test -race -tags=e2e -count=1 ./tests/e2e/...**
- Root command build: **go build ./cmd/...**
- Static analysis: **go vet ./...**
- Format check: **make check-fmt**

The CI workflow additionally tests nested Go SDK/example modules, the console, generated code, container images, and coverage. Inspect the workflow for the full authoritative list. E2E tests start containers and need Docker access.

**Important test-suite caveat:** the E2E suite currently includes executable event-before-rule-activation coverage, but also contains skipped tests for MaxDeliver exhaustion, crash-after-commit-before-ACK, multi-worker ordering, duplicate-safe effect delivery, and stale-lease fencing. A successful E2E command does not mean those skipped cases are verified. See [the E2E test matrix](e2e-test-matrix.md).

## Definition of Done

- [ ] The problem, scope, invariants, and acceptance criteria are explicit.
- [ ] Important BDD scenarios map to executable tests or are recorded as gaps.
- [ ] Bug fixes include regression tests; TDD is used where practical.
- [ ] Real dependency semantics are tested at the appropriate boundary.
- [ ] Applicable formatting, test, build, lint, and security gates were actually run.
- [ ] Skipped tests and residual risks are called out rather than hidden by a green summary.
- [ ] Tenant/security, migration, compatibility, and operational effects are reviewed.
- [ ] The PR explains the behavior guaranteed, the evidence for it, and what remains unverified.

## Keep it proportional

Use the full path for changes involving persistence, tenant authorization, event contracts, ACK/NAK behavior, retry, outbox delivery, or shard leases. For a low-risk documentation-only change, explain why runtime tests are not applicable and still check links/consistency where possible. The goal is trustworthy changes, not paperwork for its own sake.
