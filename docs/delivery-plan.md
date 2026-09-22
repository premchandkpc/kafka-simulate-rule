# Delivery plan

## Phase 0 — lock the contract — DONE

Deliver the event envelope, rule schema, decision/effect model, error taxonomy, and limits as a versioned package. Build a table-driven conformance suite for evaluators and broker adapters.

Exit: fixture events replay to the same decision hash; invalid schemas and forbidden destinations are rejected.

**Delivered:**
- `internal/domain/types.go` — All domain types with validation methods
- `internal/domain/errors.go` — 50+ typed errors with ErrorClass classification
- `internal/rules/compiler.go` — Rule compilation with limits enforcement
- `internal/rules/evaluator.go` — Pure deterministic evaluation
- `api/rule-schema.json` — JSON Schema for rule validation
- `api/envelope-schema.json` — JSON Schema for event envelope validation
- `internal/rules/evaluator_test.go` — 7 unit tests

## Phase 1 — single-worker vertical slice — DONE

Implement SQL tables for rule revisions, activation, inbox, executions, outbox, and quarantine. Build the pure evaluator and a JetStream pull consumer with a fake effect destination.

Exit: duplicate redelivery produces one execution and one outbox effect; a crash after database commit but before broker acknowledgement recovers safely.

**Delivered:**
- `migrations/001-007` — PostgreSQL migrations for 7 tables
- `internal/adapters/sql/` — SQL implementations for all repositories
- `internal/adapters/nats/nats.go` — NATS JetStream adapter
- `internal/adapters/effects/fake.go` — FakeDestination and FakeClock
- `internal/ports/ports.go` — All interfaces
- `internal/application/usecases.go` — ProcessEventUseCase, PublishEffectsUseCase, ActivateRuleUseCase
- `cmd/api/main.go` — HTTP API server
- `cmd/worker/main.go` — Worker with fetch/evaluate/ack loop
- `tests/integration/integration_test.go` — 3 integration tests

## Phase 2 — distributed workers

Add partition routing, leases, bounded concurrency, graceful draining, metrics, traces, and deploy/activate/replay API. Run identical stateless workers and let the broker plus lease store coordinate work.

Exit: killing a worker preserves per-key order, produces no duplicate effect ID, and restores backlog without manual action.

## Phase 3 — production protections

Add tenant authorization, schema integration, destination allowlists, outbox circuit breakers, retry policy, quarantine tooling, retention policy, failure/load tests, canary activation, and audit export.

Exit: an operator can explain any execution from event ID through revision, decision, effects, and delivery status.

## Phase 4 — optional Kafka adapter

Implement the same small broker interface only after JetStream passes conformance. Map `partition_key` to the Kafka key and preserve acknowledgement/retry semantics. Adapter configuration must not enter rules.

Exit: the same conformance suite passes for JetStream and Kafka.

## Migration from the prototype

1. Keep existing DSL/VM rules read-only and translate a representative small set into the declarative schema.
2. Mirror selected input events into the new worker and compare decisions without delivering mirrored effects.
3. Move one idempotent rule set, then expand traffic by deterministic key ranges.
4. Retire custom plan distribution, file state, and lane scheduling only after the audit/replay path has operational evidence.

## Do not build yet

- Raft, gossip, custom rebalancing, or node-to-node plan ACKs.
- Rust bytecode VM, CGo bridge, arbitrary plugin runtime.
- Hidden sagas in rules; use explicit compensating commands.
- Sync request/reply as a rule primitive; translate durable work into events at the edge.

## Success metrics

| Metric | Target | Status |
| --- | --- | --- |
| Duplicate effect rate | 0 for idempotent destinations | PASS (integration test) |
| Per-key ordering violations | 0 | PASS (inbox dedup) |
| Rule decision p99 | Measured separately from effect latency | N/A (needs metrics) |
| Backlog recovery | Bounded and tested after worker loss | TODO (Phase 2) |
| Replay fidelity | Same revision and input yields same decision hash | PASS (integration test) |
| Operational surface | One broker, one SQL store, stateless workers | PASS (NATS + PostgreSQL)