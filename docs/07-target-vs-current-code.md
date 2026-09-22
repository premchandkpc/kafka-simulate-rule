# Target vs current code: explicit gap analysis

This document exists to separate the architecture the project wants from the implementation it currently runs.

## 1. Target design

The target design is intentionally strong and clear:

- immutable rule revisions
- active rule pointer per tenant and rule set
- durable inbox with duplicate protection
- execution record that pins the rule revision
- outbox with retry and quarantine
- worker ack only after commit
- order by business key, not global event order

This is the story written in the project docs and the architecture narrative.

## 2. Current code reality

The current code is a partial implementation of that model.

It has a valid event-processing flow in [internal/application/usecases.go](../../internal/application/usecases.go), a domain model in [internal/domain/types.go](../../internal/domain/types.go), and a worker loop in [cmd/worker/main.go](../../cmd/worker/main.go). The remaining material gap against the target contract is:

- the runtime is not fully shard-routed by broker subject or shard ownership at startup
- the deployment flow and runtime flow are not clearly separated in operational policy

## 3. The most important gap: atomicity

The target design says the event-processing path is atomic from duplicate check through execution/outbox insert through commit.

The current implementation enforces this with a transaction-scoped `TxFactory` and repositories built from the same `*pgx.Tx`.

This is the key issue because it changes the correctness story:

- commit happens before broker ack; dedupe is safe on redelivery
- a crash before commit rolls back inbox, execution, and outbox together

This is the central local correctness guarantee. It must be preserved by future changes.

## 4. The second key gap: shard ownership vs transport

The target design describes a keyed-partition or shard-based model. The current runtime does not yet show a broker-level subject routing scheme that actually enforces shard ownership at the transport boundary.

The practical truth is:

- the worker fetches from a shared durable stream
- the system does not yet explicitly route by shard subject or shard range in code
- ordering and key isolation are therefore not yet backed by a transport-level ownership contract

That means the design is strong, but the broker + worker separation is still conceptual rather than enforced.

## 5. The tenant-scope bug (FIXED)

The rule repository now receives `tenant_scope` explicitly when saving an immutable revision; the stored identity is `(tenant_scope, rule_id, revision)`.

`ActivateRuleUseCase.Execute` passes the caller-supplied `tenantScope` to `RuleRepository.Save`. The `RuleRevision` struct carries a `TenantScope` field that is populated on read from `GetActive`. This is now correct.

The API still intentionally deploys only to scope `default`; adding a tenant-aware API is a separate control-plane feature.

## 6. The outbox claim race (FIXED)

`ClaimPending` uses a CTE that atomically selects candidates with `FOR UPDATE SKIP LOCKED` and updates them to `status='claimed'` with an owner and expiry in the same SQL statement. Two publisher replicas cannot claim the same effect because the UPDATE persists the claim before the query returns.

The pattern is:

```text
CTE: SELECT candidates ... FOR UPDATE SKIP LOCKED
UPDATE: SET status='claimed', claimed_by=$2, claim_expires_at=NOW()+1min
RETURNING ...
```

This is a single atomic statement — no separate transaction needed.

## 7. Deployment flow and runtime flow must remain separate

This is not just documentation hygiene. It is operational correctness:

- deployment flow decides whether the database and broker exist and whether migrations ran
- runtime flow decides whether one event becomes one durable execution and durable effects

A system that runs migrations on every process start is trying to flatten those two concerns together. The better design is to separate them explicitly and assign different permission and operational boundaries to each.

## 8. The honest engineering position

The honest statement is:

- the target plan is strong and directionally correct
- the runtime implementation is a partial implementation of that target
- the main remaining correctness work is broker-level shard routing and ownership before distributed-worker guarantees are added

This is exactly the point at which a senior engineer should speak clearly: the system is not yet production-equivalent to the architecture story it tells.

## 9. Fix order before claiming production-grade scaling

1. Separate migration execution from application startup in production
2. Choose and document the real shard-routing strategy
3. Add shard ownership and fencing to runtime writes
4. Add a real destination adapter, readiness checks, and observability
5. Then move to Phase 2 distributed-worker guarantees

This order matters because scaling correctness is built on top of local correctness.

## 10. Concrete examples

See `docs/examples/` for worked examples:

| Example | File | What it covers |
|---------|------|----------------|
| Event lifecycle | [01-event-lifecycle.md](examples/01-event-lifecycle.md) | Full trace from NATS to effect delivery |
| Failure scenarios | [02-failure-scenarios.md](examples/02-failure-scenarios.md) | Crash before/after commit, destination down, duplicates |
| Rule evaluation | [03-rule-evaluation.md](examples/03-rule-evaluation.md) | first_match, all_matches, otherwise, operators |
| Partitioning | [04-partitioning.md](examples/04-partitioning.md) | Shard routing, hot keys, fencing tokens |
| Revisions | [05-revisions.md](examples/05-revisions.md) | Immutable revisions, activation, retry pinning |
| Database state | [06-database-state.md](examples/06-database-state.md) | Table contents, state machines, query patterns |
| Interview scenarios | [07-interview-scenarios.md](examples/07-interview-scenarios.md) | "Why NATS?", "Why Postgres?", etc. |
