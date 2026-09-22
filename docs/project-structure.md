# Project Structure and Standards

## Go layout

```text
cmd/api/                         composition root and admin/read APIs         DONE
cmd/worker/                      fetch, shard ownership, graceful drain       DONE
internal/domain/                 entities, value objects, invariants, errors   DONE
internal/application/            use cases and transaction orchestration      DONE
internal/rules/                  schema, compiler, canonicalization, evaluator DONE
internal/ports/                  small interfaces for broker, store, clock    DONE
internal/adapters/sql/           migrations and PostgreSQL implementation     DONE
internal/adapters/nats/          NATS JetStream implementation               DONE
internal/adapters/kafka/         optional Kafka implementation               TODO
internal/adapters/effects/       HTTP/event destination implementations       DONE
internal/observability/          logs, metrics, traces, health               TODO
api/                             OpenAPI and JSON Schema contracts            DONE
tests/contract/                  shared adapter conformance fixtures          DONE
tests/integration/               real SQL/broker failure tests                DONE
migrations/                      versioned schema changes                     DONE
```

Dependencies point inward: domain has no I/O imports; application depends on ports; adapters depend on ports and external libraries; `cmd` wires concrete implementations. Avoid `pkg/` until a public API is intentionally supported.

## SOLID and DRY

Use one responsibility per bounded component. Add interfaces only at I/O, policy, or independently replaceable boundaries. Prefer concrete domain types and functions over interface-heavy indirection. Share canonical validation, error taxonomy, fixture formats, and hash logic. Do not create a universal adapter with dozens of transport flags.

## ACID and messaging rules

- No external call inside a SQL transaction.
- Ack only after commit.
- Retry uses stable identity and bounded backoff with jitter.
- Every mutable state transition has an explicit owner and invariant.
- Unique constraints and foreign keys enforce correctness at the database boundary.
- Time and randomness are injected into application policy, never the evaluator.

## Testing gates

- Unit: evaluator truth tables, canonical hashes, validation and limit cases.
- Property/fuzz: parser, canonical JSON, path resolution, malformed envelopes.
- Contract: every broker/effect adapter shares redelivery and idempotency fixtures.
- Integration: PostgreSQL transactions, concurrent duplicates, leases, outbox claiming.
- Failure: kill after commit/before ack, after send/before status, lease expiry, broker redelivery.
- Load: hot keys, skew, backlog recovery, tenant quotas, database saturation.
- Security: tenant isolation, RBAC, destination allowlist, secret redaction.

## Observability

Carry tenant ID, event ID, execution ID, rule set, revision, effect ID, virtual shard, and trace ID through records and traces. Emit metrics for accepted/rejected events, duplicate inbox conflicts, evaluation latency, broker lag, oldest backlog age, lease loss, outbox age, attempt class, quarantine count, and hot-key skew. Alert on SLO impact, not merely process restarts.