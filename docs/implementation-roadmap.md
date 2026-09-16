# Implementation Roadmap

## Phase 0: contracts and proof

Define the event envelope, rule JSON Schema, decision/effect model, error classes, identity/hash rules, and limits. Build deterministic evaluator fixtures and adapter contract fixtures.

Exit: invalid rules fail before activation; identical fixtures produce identical decision hashes.

## Phase 1: single-process vertical slice

Implement immutable rule revisions, activation, PostgreSQL migrations, inbox, execution audit, outbox, a pure evaluator, and one fake destination. Run a durable broker adapter in a single worker.

Exit: duplicate redelivery produces one execution and one effect; crash-after-commit test passes.

## Phase 2: production-shaped distribution

Add virtual shards, worker ownership, fencing tokens, bounded concurrency, graceful drain, metrics/traces, tenant quotas, and the deploy/activate/inspect/replay APIs.

Exit: killing a worker restores backlog; key ordering remains intact; a stale lease cannot commit.

## Phase 3: operational hardening

Add destination allowlists, outbox circuit breakers, retry policy, quarantine tooling, retention, object storage for large payloads, authorization, backups/PITR, dashboards, alerts, and restore/replay exercises.

Exit: operators can explain and safely replay an execution without editing database rows.

## Phase 4: Kafka adapter or managed queue adapter

Add a second transport only when its operational need is real. Reuse the same port and conformance tests. Validate keyed ordering, shutdown, retries, poison handling, and offset/ack timing under failure.

Exit: behavior matches the reference adapter without transport branches in domain code.

## Prototype migration

1. Keep the existing DSL and VM read-only.
2. Translate a representative subset into the canonical rule model.
3. Mirror events and compare decision hashes without sending effects.
4. Move one idempotent rule set by deterministic key ranges.
5. Retire plan broadcast, custom lanes, file execution state, service registry, and VM dependencies after operational evidence.

## Explicit stop conditions

Stop expanding the language when rules require service calls, loops, timers, or compensation. Those requirements belong in an application workflow or explicit event choreography. Stop scaling workers when the database or destination is the bottleneck; add capacity controls or partition/retention changes based on measurements.
