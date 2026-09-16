# Engineering principles and standards

## SOLID applied pragmatically

| Principle | Application |
| --- | --- |
| Single responsibility | Evaluator decides; store persists; broker delivers; publisher performs effects |
| Open/closed | Add broker/effect adapters behind ports; do not change domain logic |
| Liskov substitution | Every adapter passes shared delivery and idempotency contract tests |
| Interface segregation | Separate rule, execution, broker, and effect ports |
| Dependency inversion | Domain/application depend on ports; `cmd` selects SQL and broker adapters |

Do not create an interface for every struct. A port is justified only at I/O, policy, or independently replaceable boundaries.

## DRY without false abstraction

Share domain validation, errors, envelope parsing, and conformance fixtures. Do not hide Kafka and JetStream differences behind a universal set of boolean flags; each adapter owns its native details while honoring the same contract. One evaluator is authoritative; SDKs use generated models and never reimplement semantics.

## ACID and messaging rules

- One transaction owns inbox, pinned revision, decision audit, owned state changes, and outbox insertion.
- Ack a broker message only after commit.
- Never call an external system inside that transaction.
- Deterministic effect IDs are destination idempotency keys.
- Unique constraints are the final duplicate defense; caches only improve performance.
- Isolation level and locks are explicit, narrowly scoped, and covered by concurrency tests.

## Contract standards

Version public envelopes with an additive-first compatibility policy. Preserve unknown fields, publish JSON Schema or Protobuf contracts, and run producer/consumer compatibility tests in CI. Breaking changes require a new type or major version plus migration/replay plan. Timestamps are UTC RFC 3339; IDs are opaque; money is integer minor units or decimal—not binary float.

## Quality gates

| Gate | Evidence |
| --- | --- |
| Unit | Evaluator cases, property tests, parser/validator fuzzing |
| Contract | Shared fixtures for broker and effect adapters |
| Integration | Real SQL and broker redelivery/inbox/outbox tests |
| Failure | Kill worker at commit, ack, and effect boundaries |
| Load | Hot-key, partition skew, lag recovery, tenant quotas |
| Security | Authz, dependency, secret/redaction tests |
| Operations | Dashboards, alerts, runbooks, restore/replay exercise |

Use structured logs and traces with execution identifiers. Monitor acceptance rate, consumer lag and oldest age, evaluation latency, SQL failures, inbox conflicts, outbox age, effect attempts, quarantine count, and hot-key skew. Alert on sustained user impact rather than only CPU or restarts.
