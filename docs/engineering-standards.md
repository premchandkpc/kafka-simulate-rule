# Engineering principles and standards

## SOLID applied pragmatically (implemented)

| Principle | Application | Status |
| --- | --- | --- |
| Single responsibility | Evaluator decides; store persists; broker delivers; publisher performs effects | DONE |
| Open/closed | Add broker/effect adapters behind ports; do not change domain logic | DONE |
| Liskov substitution | Every adapter passes shared delivery and idempotency contract tests | DONE |
| Interface segregation | Separate rule, execution, broker, and effect ports | DONE |
| Dependency inversion | Domain/application depend on ports; `cmd` selects SQL and broker adapters | DONE |

Do not create an interface for every struct. A port is justified only at I/O, policy, or independently replaceable boundaries.

**Implemented:** `internal/ports/ports.go` - 10 interfaces for I/O boundaries.

## DRY without false abstraction (implemented)

Share domain validation, errors, envelope parsing, and conformance fixtures. Do not hide Kafka and JetStream differences behind a universal set of boolean flags; each adapter owns its native details while honoring the same contract. One evaluator is authoritative; SDKs use generated models and never reimplement semantics.

**Implemented:**
- `internal/domain/types.go` - Shared domain types
- `internal/domain/errors.go` - Error taxonomy
- `internal/rules/evaluator.go` - Single authoritative evaluator
- `api/rule-schema.json` - Rule validation schema
- `api/envelope-schema.json` - Envelope validation schema

## ACID and messaging rules (implemented)

- One transaction owns inbox, pinned revision, decision audit, owned state changes, and outbox insertion.
- Ack a broker message only after commit.
- Never call an external system inside that transaction.
- Deterministic effect IDs are destination idempotency keys.
- Unique constraints are the final duplicate defense; caches only improve performance.
- Isolation level and locks are explicit, narrowly scoped, and covered by concurrency tests.

**Implemented:**
- `internal/application/usecases.go` - ProcessEventUseCase follows the transaction boundary
- `internal/adapters/sql/inbox.go` - Unique constraint on (tenant_id, event_id)
- `internal/adapters/sql/outbox.go` - FOR UPDATE SKIP LOCKED for claiming

## Contract standards (implemented)

Version public envelopes with an additive-first compatibility policy. Preserve unknown fields, publish JSON Schema or Protobuf contracts, and run producer/consumer compatibility tests in CI. Breaking changes require a new type or major version plus migration/replay plan. Timestamps are UTC RFC 3339; IDs are opaque; money is integer minor units or decimal—not binary float.

**Implemented:**
- `api/rule-schema.json` - JSON Schema v7 for rules
- `api/envelope-schema.json` - JSON Schema v7 for event envelopes

## Quality gates

| Gate | Evidence | Status |
| --- | --- | --- |
| Unit | Evaluator cases, property tests, parser/validator fuzzing | DONE (7 tests) |
| Contract | Shared fixtures for broker and effect adapters | DONE (test suite) |
| Integration | Real SQL and broker redelivery/inbox/outbox tests | DONE (3 tests) |
| Failure | Kill worker at commit, ack, and effect boundaries | TODO (Phase 2) |
| Load | Hot-key, partition skew, lag recovery, tenant quotas | TODO (Phase 3) |
| Security | Authz, dependency, secret/redaction tests | TODO (Phase 3) |
| Operations | Dashboards, alerts, runbooks, restore/replay exercise | TODO (Phase 3) |

Use structured logs and traces with execution identifiers. Monitor acceptance rate, consumer lag and oldest age, evaluation latency, SQL failures, inbox conflicts, outbox age, effect attempts, quarantine count, and hot-key skew. Alert on sustained user impact rather than only CPU or restarts.