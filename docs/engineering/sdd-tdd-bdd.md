# Engineering Workflow: SDD, TDD, BDD, and E2E

This guide defines the minimum evidence expected for FlowRule changes. Follow [AGENTS.md](../../AGENTS.md) as the repository-wide authority. If this guide conflicts with source, tests, CI, or AGENTS.md, verify the discrepancy and update the docs rather than inventing a guarantee.

## Normative language

- **MUST** means required for merge unless an explicit, reviewed exception is recorded.
- **SHOULD** means expected unless the PR explains why it is inappropriate.
- **MAY** means optional.

## The development loop

1. **Specify (SDD):** state the problem, observable outcome, scope, non-goals, contracts, invariants, risks, and acceptance criteria.
2. **Describe (BDD):** write reviewable Given/When/Then scenarios for success, failure, and recovery paths relevant to the change.
3. **Test (TDD):** write a failing regression test, implement the smallest correct change, then refactor. If TDD is impractical, explain why and still add regression coverage.
4. **Verify:** choose tests at the boundary where the guarantee lives. Fakes prove orchestration; they do not prove PostgreSQL or NATS semantics.
5. **Review:** compare implementation, tests, schema, config, docs, rollout and rollback behavior. Report evidence without overstating it.

A scenario in Markdown is a specification, not executable coverage. Mark it covered only when an automated test asserts the relevant outcome.

## Strict change rules

### Before implementation

- **MUST** inspect AGENTS.md, relevant source and callers, tests, contracts/migrations, configuration, Makefile, Compose/Docker files, and CI.
- **MUST** identify current behavior and a measurable acceptance criterion. Reproduce a bug when practical.
- **MUST** keep the change cohesive. Avoid unrelated cleanup and speculative abstractions.
- **MUST** assess API/event/SDK compatibility, tenant isolation, transaction boundaries, idempotency, retries, cancellation, shutdown, observability, deployment and rollback limits where relevant.
- **SHOULD** record a plan for multi-package, architectural, data-model, or operationally risky work. Use [the change-plan template](change-plan-template.md).

### During implementation

- **MUST** preserve dependency direction and existing ownership. Reuse existing contracts and helpers when their semantics match.
- **MUST** add regression tests for bug fixes and update affected implementations, fakes, examples, generated outputs and docs where applicable.
- **MUST NOT** claim exactly-once external effects, strict global/per-key ordering, tenant security, or fencing correctness without tests at the actual guarantee boundary.
- **MUST NOT** silently weaken validation, retry policy, authorization, or invariants to make a test pass.
- **SHOULD** use explicit synchronization and bounded polling for concurrency tests, not arbitrary sleeps.

### Before merge

- **MUST** format changed files and run applicable focused checks before broader checks.
- **MUST** run relevant root and nested-module checks; root-level ./... does not test nested Go modules.
- **MUST** inspect the full diff for stale docs, duplicate logic, misplaced files, unsafe defaults, and unrelated churn.
- **MUST** check CI for the exact PR head SHA. Queued, in-progress, skipped, or missing checks are not passing checks.
- **MUST** report commands and actual outcomes, including failures, skips, checks not run, and residual risk.
- For documentation-only changes, runtime tests do not validate docs. **MUST** verify paths, links, commands/flags, and claims against source and CI.

## FlowRule guarantees to verify

These are review prompts, not assertions that every behavior is already implemented.

| Concern | Required question |
| --- | --- |
| Tenant isolation | Is identity derived from a trusted boundary and applied to every relevant read/write? |
| Inbox and execution | Can duplicate delivery create a second logical execution? Are uniqueness and transaction boundaries tested? |
| Inbox/execution/outbox | Do required durable writes commit atomically? What happens when commit outcome is uncertain? |
| Message ACK | Is ACK sent only after durable commit? What does redelivery do after a crash? |
| Retry and ordering | Which failures are retryable? Can a later event overtake a failed event for the same key? What happens at exhaustion? |
| Lease fencing | Can an old worker commit after lease ownership changes? Is the storage boundary enforcing the token? |
| Rule revisions | Is the selected immutable revision explicit and persisted consistently? |
| External effects | Is delivery at-least-once? Does the destination actually honor the idempotency key? |
| Operations | Are retries, quarantine, lease loss and delivery failures observable without leaking secrets or tenant payloads? |

## Choose the test boundary

| Test layer | Evidence it can provide | It cannot prove alone |
| --- | --- | --- |
| Unit | Pure logic, validation, deterministic decisions | Database isolation, broker redelivery, process crashes |
| Service/component | Use-case orchestration with fakes | Real adapter or infrastructure behavior |
| Adapter/contract | SQL/query mapping, serialization, adapter contracts | End-to-end lifecycle unless exercised |
| Integration | Interactions among wired components | Real dependency semantics when dependencies are mocked |
| E2E | Actual worker path with dependencies configured by its harness | Skipped scenarios or real external-destination behavior when a fake sender is used |

## Useful root-module commands

Run from the repository root, and check [CI](../../.github/workflows/go-ci.yml) and the Makefile for the authoritative complete set.

- Unit-focused package tests: **go test -race -count=1 ./internal/...**
- Mock-based integration/service suite: **go test -race -tags=integration -count=1 ./tests/integration/...**
- PostgreSQL + NATS E2E suite: **go test -race -tags=e2e -count=1 ./tests/e2e/...**
- Build commands: **go build ./cmd/...**
- Static analysis: **go vet ./...**
- Formatting check: **make check-fmt**

The E2E suite uses Testcontainers and requires Docker. CI also checks nested Go modules, console, generated code, container images, and coverage. Do not infer those checks from the commands above.

Critical E2E cases currently include skipped tests for MaxDeliver exhaustion, crash-after-commit-before-ACK, multi-worker ordering, duplicate-safe effect delivery, and stale-lease fencing. See [the E2E test matrix](e2e-test-matrix.md). A successful test command does not turn skipped tests into evidence.

## Definition of Done

- [ ] Requirement, scope, non-goals and acceptance criteria are explicit.
- [ ] Relevant BDD scenarios map to executable tests or clearly marked gaps.
- [ ] Bug fixes have regression coverage; any TDD exception is explained.
- [ ] Tests exercise the boundary needed for the claimed guarantee.
- [ ] Relevant format, build, test, lint, security and CI checks are reported accurately.
- [ ] Security, compatibility, migration, deployment and rollback impacts are reviewed where relevant.
- [ ] Documentation matches implementation and uses no stronger claims than the evidence.
- [ ] Remaining risks, skipped tests and unverified behavior are explicit.
