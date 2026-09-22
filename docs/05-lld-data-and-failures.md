# Story 05 — LLD, data, and failure handling

## Code layers

```text
cmd/api, cmd/worker        composition and process loops
internal/application       ProcessEvent, PublishEffects, ActivateRule
internal/rules             compile/validate and pure evaluation
internal/domain            envelopes, rules, decisions, IDs and errors
internal/ports             broker, repositories, clock, sender interfaces
internal/adapters/sql      PostgreSQL repositories and migrations
internal/adapters/nats     JetStream consumer/publisher
internal/adapters/effects  fake/incomplete HTTP destinations
```

Dependencies point inward. The evaluator has no database, broker, clock, network, or random dependency, so a given compiled revision and event can be tested deterministically.

## Persistent model

| Table | Key | Purpose |
| --- | --- | --- |
| `rule_revisions` | `(tenant_scope, rule_id, revision)` | Immutable source and compiled form |
| `rule_activations` | `(tenant_scope, rule_set)` | Active revision pointer/version |
| `inbox` | `(tenant_id, event_id)` | Duplicate defense and commit marker |
| `executions` | `execution_id` | Rule revision, decision hash, status and audit fields |
| `outbox_effects` | `effect_id` | Pending/delivered/quarantined side effects |
| `shard_leases` | `virtual_shard` | Future exclusive shard ownership/fencing |
| `quarantine` | `quarantine_id` | Intended durable poison-item record |

## Required atomic processing algorithm

This is the implementation to reach before production:

```text
BEGIN
  insert/lock inbox(tenant_id,event_id)
  on duplicate: return the prior completed execution
  read activation and pinned revision
  evaluate with no I/O
  insert execution and all deterministic outbox effects
  mark inbox committed with execution ID
COMMIT
ACK JetStream only after COMMIT
```

Use a single transaction-owning repository/use case. Keep the external send after commit; never make an HTTP/NATS call inside the transaction. On a duplicate still marked `processing`, safely resume/repair it rather than treating it as a normal completed duplicate.

## Current implementation gaps to fix

1. ~~Separate SQL calls break the required transaction and leave a crash window.~~ **FIXED.** `ProcessEventUseCase.Execute` now runs inbox insert → activation read → rule read → evaluation → execution insert → outbox insert → inbox mark-committed inside a single `sql.Tx`. The broker ACK happens only after commit. Repositories accept `ports.Querier` (satisfied by both `*pgxpool.Pool` and `*pgx.Tx`), and a `TxFactory` + `RepoFactory` inject the transaction boundary without coupling the use case to a specific driver.
2. ~~`ActivationRepository.Get` and `RuleRepository.GetActive` return a database error for no rows, not a clean `nil`; the intended domain error path is therefore unreliable.~~ **FIXED.** Both methods now return `nil, nil` when no rows match, consistent with `InboxRepository.Get` and `ExecutionRepository.Get`.
3. Rule save stores `tenant_scope = revision.RuleID`, not the requested tenant scope, so non-default/tenant-specific activation cannot work correctly.
4. `ClaimPending` uses `FOR UPDATE SKIP LOCKED` without an explicit transaction or claim-state update; locks are released when the query ends and competing publishers can send the same effect concurrently.
5. `HTTPDestination.Send` does not make an HTTP call, and the worker uses only `FakeDestination`.
6. Quarantine is held in an in-memory fake repository; replay is absent.
7. Worker processing is sequential, has no graceful drain of in-flight work, and does not use the existing shard-lease repository.

## Failure rules to retain

| Failure point | Correct response |
| --- | --- |
| Before SQL commit | Roll back; broker redelivers |
| After commit, before ACK | Redelivery finds inbox/execution; ACK without reevaluation |
| Destination accepted, status update failed | Retry same `effect_id`; destination deduplicates |
| Repeated retryable delivery error | Backoff with jitter, then persist quarantine |
| Invalid/unrecoverable event | Persist event/error/revision context in quarantine; do not block other keys |

Instrument broker lag/oldest-message age, evaluation latency/error class, SQL transaction/conflict time, pending outbox age, delivery attempts, quarantine count, tenant quotas, and shard ownership. None of that telemetry exists yet.
