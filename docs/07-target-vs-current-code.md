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

## 5. The tenant-scope reliability issue

The rule repository now receives `tenant_scope` explicitly when saving an immutable revision; the stored identity is `(tenant_scope, rule_id, revision)`.

However, `ActivateRuleUseCase.Execute` has a bug: it passes `revision.RuleID` as the `tenant_scope` argument to `RuleRepository.Save`, so non-default tenants silently write rules under the wrong scope. This is tracked as gap 3 in Story 05.

The API still intentionally deploys only to scope `default`; adding a tenant-aware API is a separate control-plane feature.

## 6. The outbox claim race

The outbox `ClaimPending` currently uses `SELECT ... FOR UPDATE SKIP LOCKED` without a durable status change. Row locks release when the query returns, so two publisher replicas can claim and send the same effect concurrently. This is tracked as gap 4 in Story 05.

The correct pattern (not yet implemented) is:

```text
claim in a short tx -> update status='claimed' and owner -> commit
send outside the tx
mark delivered or retry in a new short tx
```

This is the actual contract that makes the outbox safe.

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
