# Design

## High-level Design

FlowRule guarantees **domain-level exactly-once processing**: business events are processed exactly once even when the broker delivers them at least once.

### Core Guarantee

```
At-least-once broker delivery
        │
        ▼
┌───────────────────┐
│  Transactional    │  ← inbox dedup prevents duplicate execution
│  Inbox            │     (unique constraint on tenant_id + event_id)
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│  Rule Evaluation  │  ← pure, deterministic, no side effects
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│  Execution Record │  ← immutable decision hash, pinned revision
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│  Outbox Effects   │  ← deterministic effect IDs = idempotency keys
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│  Broker ACK       │  ← only after commit
└───────────────────┘
```

### Bounded Contexts

| Context | Aggregate | Key Invariant | State |
|---------|-----------|---------------|-------|
| Rule management | RuleRevision | One active revision per (tenant_scope, rule_set) | Immutable once saved |
| Event intake | InboxEntry | One execution per (tenant_id, event_id) | processing → committed |
| Decisioning | Execution | Immutable decision hash, pinned revision | pending → completed/failed/quarantined |
| Effect delivery | OutboxEffect | At-least-once with idempotent effect_id | pending → claimed → delivered/quarantined |
| Operations | ShardLease | One owner per virtual shard with fencing token | Active/expired |

### Consistency Model

ACID applies inside SQL, not across SQL + broker + HTTP. The transaction boundary is intentionally narrow:

```
BEGIN
  ┌─────────────────────────────────────────────────────────┐
  │ 1. inbox insert (dedup check)                          │
  │ 2. activation read (find active rule set)              │
  │ 3. rule read (get compiled revision)                   │
  │ 4. evaluate (pure function, no I/O)                    │
  │ 5. execution insert (record decision)                  │
  │ 6. outbox insert (queue effects)                       │
  │ 7. inbox mark committed                                │
  └─────────────────────────────────────────────────────────┘
COMMIT
  → broker ACK only after commit succeeds
```

**Why this works:**
- Inbox unique constraint prevents duplicate execution creation
- If crash before commit: rollback, broker redelivers, inbox insert succeeds on retry
- If crash after commit but before ACK: inbox already committed, redelivery finds existing execution, returns it, ACKs
- Outbox effects have deterministic IDs → safe to re-deliver

## Low-level Design

### Core Types

| Type | Package | Fields | Purpose |
|------|---------|--------|---------|
| EventEnvelope | domain | ID, Type, TenantID, PartitionKey, OccurredAt, Data, Headers | Inbound event |
| RuleRevision | domain | TenantScope, RuleID, Revision, Source, Compiled, Hash, InputContract | Compiled rules + metadata |
| CompiledRule | domain | ID, Priority, When, Then, Name, Description, Tags | Single rule |
| Predicate | domain | Path, Op, Value, All, Any, Not | Leaf or composite condition |
| Action | domain | EmitAction, CommandAction | What to do on match |
| Effect | domain | ID, ExecutionID, Destination, Name, Payload, EffectType | Emitted effect |
| Decision | domain | Matched, Effects, Hash, Explanations | Evaluation result |
| Execution | domain | ID, EventID, TenantID, RuleSet, Revision, DecisionHash, Status | Processing record |
| OutboxEffect | domain | ID, ExecutionID, Destination, Name, Payload, Status, Attempts | Delivery record |
| InboxEntry | domain | TenantID, EventID, Status, ExecutionID, FirstSeenAt, CommittedAt | Dedup record |
| ShardLease | domain | VirtualShard, Owner, FencingToken, ExpiresAt, RoutingEpoch | Worker ownership |
| QuarantineEntry | domain | ID, SourceType, SourceID, ErrorClass, PayloadRef, EventID | Failed item |
| RuleActivation | domain | TenantScope, RuleSet, Revision, Version, Actor, ActivatedAt | Active revision pointer |

### Domain Behavior: State Machines

State transitions are enforced at the domain layer. Invalid transitions return errors.

#### Execution

```
                     ┌──────────────┐
                     │   pending    │
                     └──────┬───────┘
                            │
               ┌────────────┼────────────┐
               ▼            ▼            ▼
         ┌──────────┐ ┌──────────┐ ┌──────────────┐
         │completed │ │  failed  │ │ quarantined  │
         └──────────┘ └──────────┘ └──────────────┘
```

```go
exec.Complete(now)    // pending → completed
exec.Fail(now, msg)   // pending → failed
exec.Quarantine(now, msg) // pending → quarantined
```

#### OutboxEffect

```
        ┌──────────┐
        │ pending  │◄─────────────────┐
        └────┬─────┘                  │
             │ claim                  │ retry
             ▼                        │
        ┌──────────┐                  │
        │ claimed  │──────────────────┘
        └────┬─────┘
             │
      ┌──────┼──────┐
      ▼      ▼      ▼
┌─────────┐ ┌──────────┐ ┌──────────────┐
│delivered│ │ pending  │ │ quarantined  │
└─────────┘ │ (retry)  │ └──────────────┘
            └──────────┘
```

```go
effect.Claim(owner, now)     // pending → claimed
effect.Deliver(now)          // claimed → delivered
effect.Retry(now, delay, attempts, msg) // claimed → pending
effect.Quarantine(now, msg)  // claimed → quarantined
```

#### InboxEntry

```
┌────────────┐     ┌──────────┐
│ processing │────>│ committed│
└────────────┘     └──────────┘
```

```go
entry.Commit(executionID, now) // processing → committed
entry.IsDuplicate()            // true if committed with execution ID
```

### Transaction Pattern

```go
type TxRepos struct {
    Inbox       ports.InboxRepository
    Activations ports.ActivationRepository
    RuleRepo    ports.RuleRepository
    Executions  ports.ExecutionRepository
    Outbox      ports.OutboxRepository
}

// RepoFactory creates transaction-scoped repositories.
// The concrete type is an adapter concern (sql.Tx implements this).
type RepoFactory func(db interface{}) TxRepos
```

The `RepoFactory` receives a `sql.Tx` (wrapping pgx.Tx) and constructs repositories scoped to that transaction. The application never manages connection pooling or transaction lifecycle directly.

### Data Model (PostgreSQL)

```sql
-- Immutable rule source and compiled form
CREATE TABLE rule_revisions (
    tenant_scope  TEXT NOT NULL,
    rule_id       TEXT NOT NULL,
    revision      BIGINT NOT NULL,
    source        JSONB NOT NULL,
    compiled      JSONB NOT NULL,
    content_hash  TEXT NOT NULL,
    compiler_version TEXT NOT NULL,
    match_mode    TEXT NOT NULL,
    input_contract JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_scope, rule_id, revision)
);

-- Active revision per rule set
CREATE TABLE rule_activations (
    tenant_scope  TEXT NOT NULL,
    rule_set      TEXT NOT NULL,
    revision      BIGINT NOT NULL,
    version       BIGINT NOT NULL DEFAULT 1,
    actor         TEXT NOT NULL,
    activated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_scope, rule_set),
    FOREIGN KEY (tenant_scope, rule_set, revision) REFERENCES rule_revisions(...)
);

-- Dedup and processing status
CREATE TABLE inbox (
    tenant_id     TEXT NOT NULL,
    event_id      TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'processing',
    execution_id  TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    committed_at  TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, event_id)
);

-- Processing record with decision audit
CREATE TABLE executions (
    execution_id  TEXT PRIMARY KEY,
    event_id      TEXT NOT NULL,
    tenant_id     TEXT NOT NULL,
    rule_set      TEXT NOT NULL,
    revision      BIGINT NOT NULL,
    decision_hash TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    error         TEXT,
    trace_id      TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ
);

CREATE INDEX idx_executions_tenant_event ON executions (tenant_id, event_id);
CREATE INDEX idx_executions_status ON executions (status);

-- Durable effects with delivery state
CREATE TABLE outbox_effects (
    effect_id      TEXT PRIMARY KEY,
    execution_id   TEXT NOT NULL REFERENCES executions(execution_id),
    destination    TEXT NOT NULL,
    name           TEXT NOT NULL,
    payload        JSONB NOT NULL,
    effect_type    TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    attempts       INT NOT NULL DEFAULT 0,
    max_attempts   INT NOT NULL DEFAULT 5,
    available_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_by     TEXT,
    claimed_at     TIMESTAMPTZ,
    claim_expires_at TIMESTAMPTZ,
    last_error     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_effects_status ON outbox_effects (status, available_at);
CREATE INDEX idx_outbox_effects_execution ON outbox_effects (execution_id);
CREATE INDEX idx_outbox_effects_claim_expiry ON outbox_effects (status, claim_expires_at) WHERE status = 'claimed';

-- Worker ownership with fencing tokens
CREATE TABLE shard_leases (
    virtual_shard  INT PRIMARY KEY,
    owner          TEXT NOT NULL,
    fencing_token  BIGINT NOT NULL DEFAULT 1,
    expires_at     TIMESTAMPTZ NOT NULL,
    routing_epoch  BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX idx_shard_leases_owner ON shard_leases (owner);
CREATE INDEX idx_shard_leases_expires ON shard_leases (expires_at);

-- Failed items for inspection and replay
CREATE TABLE quarantine (
    quarantine_id  TEXT PRIMARY KEY,
    source_type    TEXT NOT NULL,
    source_id      TEXT NOT NULL,
    error_class    TEXT NOT NULL,
    payload_ref    TEXT,
    event_id       TEXT,
    tenant_id      TEXT,
    revision       BIGINT,
    decision_hash  TEXT,
    error_message  TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_quarantine_tenant ON quarantine (tenant_id);
CREATE INDEX idx_quarantine_created ON quarantine (created_at);
```

### Effect ID (Deterministic Idempotency Key)

Effect IDs are deterministic and serve as natural idempotency keys for destinations:

```
effect_id = sha256(tenant_id + "|" + event_id + "|" + rule_set + "|" + revision + "|" + rule_id + "|" + action_index)
```

This makes them stable across replays. Replaying the same event produces the same effect IDs, allowing destinations to deduplicate.

### Decision Hash

```
decision_hash = sha256(rule_set + revision + sorted(matched_rule_ids) + event_hash + sorted(effect_ids))
```

Stable across identical inputs. Used for:
- Audit trail
- Replay verification (re-evaluating produces same hash)
- Detecting rule drift

### Partitioning & Ordering

**Virtual Shards**: 4096 fixed shards mapped to physical workers.

```
shard = fnv32a(tenant_id + ":" + partition_key) % 4096
```

**Ordering guarantees:**
- Events with same `partition_key` → same shard → processed by same worker sequentially
- Ordering guaranteed *within a key*, never across keys
- Shard ownership via `shard_leases` with fencing tokens prevents split-brain

**Rebalancing:**
1. Mark instance draining
2. Stop new fetches
3. Finish bounded in-flight executions
4. Release/expire leases
5. Terminate

### Failure Handling

| Scenario | Behavior |
|----------|----------|
| Crash before commit | Rollback, broker redelivers, inbox dedup catches it |
| Crash after commit, before ACK | Inbox committed, redelivery returns existing execution, no duplicate |
| Destination down | Exponential backoff (1s, 2s, 4s, 8s, 16s capped at 60s), max 5 attempts → quarantine |
| Poison message | Permanent errors (invalid envelope, rule not found) acked immediately; transient retried |
| Two workers same event | Inbox unique constraint prevents duplicate execution |
| Outbox publisher crash | Claimed effects have TTL (claim_expires_at), become claimable again after expiry |
| Worker loses lease | Fencing token prevents stale commits; new owner takes over |

### Explanation of Exactly-Once

The exactly-once guarantee is achieved at the **business level** (not transport level):

1. **Transport**: NATS delivers at-least-once
2. **Dedup**: Inbox unique constraint ensures at-most-one execution per (tenant_id, event_id)
3. **Idempotent effects**: Deterministic effect IDs allow destinations to deduplicate
4. **Commit ordering**: Broker ACK only after DB commit → no lost events, no duplicates

This is "effectively exactly-once" — the business outcome occurs exactly once.

### What the Evaluator Does NOT Do

The evaluator is a **pure function**:

- No database reads
- No HTTP/gRPC calls
- No clock access
- No random numbers
- No global state mutation
- No external service calls

Given the same compiled revision and event, it **always** produces the same decision.

### Contract Validation

Rule revisions can declare an `input_contract`. Before activation, `ValidatePathsAgainstContract` checks that all predicate paths exist in the contract schema. This prevents activating rules that reference non-existent fields.

```json
{
  "rule_set": "order.created",
  "revision": 2,
  "input_contract": {
    "name": "OrderEvent",
    "version": "1.0",
    "schema_hash": "a1b2c3d4"
  }
}
```

### Compiler Limits (Configurable)

| Limit | Default | Description |
|-------|---------|-------------|
| MaxRulesPerSet | 100 | Maximum rules in a single rule set |
| MaxPredicates | 200 | Total predicates across all rules |
| MaxNestingDepth | 10 | Maximum nesting depth for composite predicates |
| MaxActionsPerSet | 10 | Total actions across all rules |
| MaxPayloadBytes | 256KB | Maximum event payload size |
| MaxRuleIDLen | 128 | Maximum rule ID length |

### Path Resolution

Paths use dot notation starting with `$`:

| Path | Resolves to |
|------|-------------|
| `$.id` | `event.data.id` |
| `$.order.total` | `event.data.order.total` |
| `$.items` | `event.data.items` (entire array) |

**V1 limitations:**
- No array indexing (`$.items[0]` not supported)
- No recursive queries
- Dots only (no bracket notation)