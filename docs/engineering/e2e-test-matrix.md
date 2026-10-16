# E2E Test Matrix: FlowRule

This document separates current test evidence from desired behavior. A test command returning exit code zero is not proof for scenarios that are skipped, use mocks, or are not asserted.

## Harness and commands

The E2E suite is in tests/e2e and is guarded by the Go build tag e2e. Its harness starts PostgreSQL and NATS JetStream using Testcontainers, initializes the configured storage, starts a worker, and uses a fake effect sender for external effects. Therefore it exercises the database/broker worker path, but does not prove real third-party destination behavior.

Run from the repository root:

- E2E: **go test -race -tags=e2e -count=1 ./tests/e2e/...**
- Mock-based service/integration suite: **go test -race -tags=integration -count=1 ./tests/integration/...**
- Unit suite: **go test -race -count=1 ./internal/...**

The CI workflow runs the E2E suite with race detection and coverage. Docker must be available to run the Testcontainers suite locally.

## Acceptance matrix

| Behavior | Existing evidence | Status / remaining work |
| --- | --- | --- |
| Event arrives before rule activation | tests/e2e/event_before_rule_test.go: TestEventBeforeRuleActivation_IsNotLost | Executable E2E test. Verify it remains stable and deterministic. |
| Duplicate event delivery creates one execution | tests/integration/integration_test.go has mock-backed service coverage | Service-level evidence only; add a PostgreSQL + JetStream redelivery test. |
| Transaction rollback leaves no partial inbox/execution/outbox state | Existing service tests use fakes; no live rollback scenario identified in the E2E file | Gap: inject a persistence failure inside the transaction and assert all related tables. |
| MaxDeliver exhaustion is visible in quarantine | TestEventBeforeRuleActivation_WithMaxDeliver is skipped | Gap: configure consumer MaxDeliver and assert quarantine/no silent strand. |
| Crash after commit but before ACK | TestCrashAfterCommitBeforeAck is skipped | Gap: inject a worker stop between durable commit and ACK, redeliver, assert one execution and stable effects. |
| Multi-worker per-key ordering | TestConcurrentWorkersPerKeyOrder is skipped | Gap: start multiple workers, force retry/lease handoff, and assert order from persisted sequence/state rather than timing alone. |
| External effect duplicate safety | TestEffectDeliveryNoDuplicates is skipped and harness uses a fake sender | Gap: use a local idempotent HTTP receiver that records effect IDs; test response loss and retry. |
| Stale lease holder is fenced | TestStaleLeaseHolderFenced is skipped | Gap: force lease takeover, resume old holder, and assert the storage transaction rejects its stale token. |
| Cross-tenant request cannot read or mutate another tenant | Handler/adapter unit tests exist, but this E2E harness does not model production trusted-proxy authentication | Partial: preserve focused tests and add an authenticated end-to-end negative test when auth wiring is testable. |
| Rule revision remains consistent during concurrent activation | No matching E2E assertion identified in the current E2E file | Gap: synchronize revision activation with processing and assert the persisted revision selected by the execution. |

## How to close a gap

For each row, make the assertion deterministic and state-based:

1. Identify the seam where the failure or race can be injected.
2. Add a bounded test hook or controllable fake only for the seam; keep PostgreSQL/NATS real when their semantics are under test.
3. Assert persisted rows, ownership/fencing state, message settlement, or receiver-observed idempotency IDs.
4. Run the focused tagged test repeatedly, then the full E2E suite with the race detector.
5. Remove temporary diagnostics and ensure the test has a bounded timeout and cleanup.
6. Update this matrix when the test is executable, not when it is merely written or unskipped.

Do not add arbitrary sleeps as synchronization. Poll for an observable state with a deadline or coordinate actors with explicit channels/barriers. Do not change production guarantees merely to make a test pass.

## Current scope caveats

- The E2E fake effect sender records/logs delivery but is not an external HTTP receiver with deduplication semantics.
- Several critical test functions currently call t.Skip; they are debt, not coverage.
- The mock-based integration package is useful for service behavior but cannot prove PostgreSQL transaction isolation or NATS redelivery/ACK semantics.
- The README and AGENTS.md explicitly warn that end-to-end claims need evidence from the actual dependency boundary.
