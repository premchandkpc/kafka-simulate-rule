# Implementation Roadmap

## Phase 0: contracts and proof — DONE

Define the event envelope, rule JSON Schema, decision/effect model, error classes, identity/hash rules, and limits. Build deterministic evaluator fixtures and adapter contract fixtures.

Exit: invalid rules fail before activation; identical fixtures produce identical decision hashes.

**Delivered:**
- `internal/domain/types.go` — EventEnvelope, RuleRevision, Effect, Decision, Execution, OutboxEffect, InboxEntry, ShardLease, QuarantineEntry
- `internal/domain/errors.go` — Error taxonomy with 50+ typed errors and ErrorClass classification
- `internal/rules/compiler.go` — Validates, compiles, and hashes rule sources with limits enforcement
- `internal/rules/evaluator.go` — Pure deterministic evaluation with path resolution and 10 operators
- `api/rule-schema.json` — JSON Schema for rule validation
- `api/envelope-schema.json` — JSON Schema for event envelope validation
- `internal/rules/evaluator_test.go` — 7 unit tests covering evaluator and compiler

## Phase 1: single-process vertical slice — DONE

Implement immutable rule revisions, activation, PostgreSQL migrations, inbox, execution audit, outbox, a pure evaluator, and one fake destination. Run a durable broker adapter in a single worker.

Exit: duplicate redelivery produces one execution and one effect; crash-after-commit test passes.

**Delivered:**
- `migrations/001-007` — PostgreSQL tables for rules, activations, inbox, executions, outbox, leases, quarantine
- `internal/adapters/sql/` — SQL implementations for all repositories
- `internal/adapters/nats/nats.go` — NATS JetStream consumer and publisher
- `internal/adapters/effects/fake.go` — FakeDestination and FakeClock for testing
- `internal/ports/ports.go` — Interfaces for BrokerConsumer, Delivery, repositories, EffectSender, Clock
- `internal/application/usecases.go` — ProcessEventUseCase, PublishEffectsUseCase, ActivateRuleUseCase
- `cmd/api/main.go` — HTTP API server for rule deployment
- `cmd/worker/main.go` — Worker with fetch/evaluate/ack loop
- `tests/integration/integration_test.go` — 3 integration tests (duplicate redelivery, deterministic hash, effect ID)

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