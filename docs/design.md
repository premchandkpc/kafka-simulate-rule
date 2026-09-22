# Design

## High-level design

FlowRule guarantees domain-level exactly-once processing: business events are processed exactly once even when the broker delivers them at least once.

### Bounded contexts

| Context | Aggregate | Key invariant |
|---------|-----------|---------------|
| Rule management | RuleRevision | One active revision per (tenant_scope, rule_set) |
| Event intake | InboxEntry | One execution per (tenant_id, event_id) |
| Decisioning | Execution | Immutable decision hash, pinned revision |
| Effect delivery | OutboxEffect | At-least-once with idempotent effect_id |
| Operations | ShardLease | One owner per virtual shard with fencing token |

### Consistency model

ACID applies inside SQL, not across SQL + broker + HTTP. The transaction boundary is intentionally narrow:

```
BEGIN
  inbox insert -> activation read -> rule read -> evaluate
  -> execution insert -> outbox insert -> inbox mark committed
COMMIT
-- broker ACK only after commit
```

## Low-level design

### Core types

| Type | Package | Purpose |
|------|---------|---------|
| EventEnvelope | domain | Inbound event with ID, type, tenant, partition key, data |
| RuleRevision | domain | Compiled rules + metadata + hash + contract |
| CompiledRule | domain | ID, priority, when/then, name, description, tags |
| Predicate | domain | Path/op/value leaf or all/any/not composite |
| Decision | domain | Matched rules, effects, hash, explanations |
| Execution | domain | Pinned revision, decision hash, status, trace |
| OutboxEffect | domain | Destination, payload, status, attempts, backoff |
| InboxEntry | domain | Tenant/event ID, status, first seen, committed at |
| ShardLease | domain | Owner, fencing token, expiry, routing epoch |
| QuarantineEntry | domain | Error class, payload ref, source, replay audit |

### Domain behavior

State machines are enforced at the domain layer:

- `Execution`: pending -> completed | failed | quarantined
- `OutboxEffect`: pending -> claimed -> delivered | pending (retry) | quarantined
- `InboxEntry`: processing -> committed

### Transaction pattern

```go
type TxRepos struct {
    Inbox       ports.InboxRepository
    Activations ports.ActivationRepository
    RuleRepo    ports.RuleRepository
    Executions  ports.ExecutionRepository
    Outbox      ports.OutboxRepository
}

type RepoFactory func(db interface{}) TxRepos
```

The `RepoFactory` receives a `sql.Tx` (wrapping pgx.Tx) and constructs repositories scoped to that transaction. The application never manages connection pooling or transaction lifecycle directly.

### Data model

| Table | Primary key | Purpose |
|-------|-------------|---------|
| rule_revisions | (tenant_scope, rule_id, revision) | Immutable source, compiled form, hash, validation |
| rule_activations | (tenant_scope, rule_set) | Active revision, monotonic version |
| inbox | unique(tenant_id, event_id) | Dedup and processing status |
| executions | execution_id | Pinned revision, decision hash, trace |
| outbox_effects | effect_id | Durable effects, attempt/status metadata |
| shard_leases | virtual_shard | Owner, fencing token, expiry |
| quarantine | quarantine_id | Error category, attempts, replay audit |

### Effect ID

Effect IDs are deterministic: `sha256(execution_id + destination + name + payload)`. This makes them natural idempotency keys for destinations.

### Decision hash

`sha256(rule_set + revision + sorted rule IDs + event hash)`. Stable across identical inputs. Used for audit and replay pinning.
