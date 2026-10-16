# E2E Test Matrix: FlowRule

**Purpose:** distinguish implemented test evidence from desired behavior and known gaps. A green command does not cover skipped tests.

## Harness and scope

The tagged suite in tests/e2e uses Testcontainers for PostgreSQL and NATS JetStream. Its fake effect sender does not establish real HTTP destination idempotency. Check the current harness and CI before relying on these details.

Run from the repository root:

- E2E: **go test -race -tags=e2e -count=1 ./tests/e2e/...**
- Mock-based integration/service suite: **go test -race -tags=integration -count=1 ./tests/integration/...**
- Focused unit tests: **go test -race -count=1 ./internal/...**

Docker is required for the Testcontainers E2E suite. The CI workflow is authoritative for the full set of checks, including nested modules.

## Acceptance matrix

| Behavior | Current evidence | Status | Next proof required |
| --- | --- | --- | --- |
| Event arrives before rule activation | tests/e2e/event_before_rule_test.go:TestEventBeforeRuleActivation_IsNotLost | **Covered by E2E** | Keep deterministic and stable. |
| Duplicate event produces one execution | Mock-backed service coverage in tests/integration/integration_test.go | **Partial** | PostgreSQL + JetStream redelivery test; assert persisted execution/outbox state. |
| Failed transaction leaves no partial state | No live rollback case identified in the E2E suite | **Gap** | Inject a write failure and assert inbox/execution/outbox rollback. |
| MaxDeliver exhaustion is handled | TestEventBeforeRuleActivation_WithMaxDeliver is skipped | **Skipped** | Assert configured delivery limit, quarantine/recovery, and no silent strand. |
| Crash after commit, before ACK | TestCrashAfterCommitBeforeAck is skipped | **Skipped** | Inject stop between commit and ACK; redeliver; assert one execution and stable effects. |
| Multi-worker per-key ordering | TestConcurrentWorkersPerKeyOrder is skipped | **Skipped** | Force retry/lease handoff; assert persisted order rather than timing. |
| External effect duplicate safety | TestEffectDeliveryNoDuplicates is skipped; sender is fake | **Skipped / partial** | Use local idempotent HTTP receiver; simulate response loss and retry. |
| Stale lease fencing | TestStaleLeaseHolderFenced is skipped | **Skipped** | Take over lease, resume stale worker, assert storage rejects old token. |
| Cross-tenant read/write isolation | Focused handler/adapter tests; E2E harness does not model production trusted-proxy auth | **Partial** | Add authenticated negative E2E test when auth wiring is testable. |
| Revision consistency during activation | No matching E2E assertion identified | **Gap** | Synchronize activation and processing; assert persisted selected revision. |

Statuses describe evidence found during the PR review, not a permanent guarantee. Re-check test source before changing a status.

## Rules for closing a gap

1. Identify the exact failure/race seam.
2. Keep PostgreSQL/NATS real when testing their semantics; introduce a narrow deterministic hook only where needed.
3. Assert persisted rows, message settlement, fencing state, or receiver-observed idempotency IDs.
4. Use explicit barriers or bounded polling, not arbitrary sleeps.
5. Add bounded timeouts and cleanup; run the focused test repeatedly, then the full E2E suite with race detection.
6. Remove the skip only when the intended assertion is implemented and stable.
7. Update this matrix in the same PR that adds executable coverage.

Do not weaken production guarantees to make a test pass. Do not describe fake-based coverage as live infrastructure proof.
