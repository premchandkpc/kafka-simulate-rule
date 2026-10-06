# FlowRule

A distributed rules engine that evaluates business events against configurable rule sets and produces durable effects with exactly-once semantics.

## What it does

```
                       ┌─────────────────────────────────────────────┐
                       │              FlowRule Engine                │
                       │                                             │
     ┌──────────┐     │  ┌─────────┐   ┌──────────┐   ┌─────────┐ │     ┌──────────────┐
     │  Event   │────>│  │  Inbox   │──>│ Evaluator │──>│ Outbox  │─│────>│  Destination │
     │  (NATS)  │     │  │  Dedup   │   │  Rules   │   │ Effects │ │     │  (HTTP/gRPC) │
     └──────────┘     │  └─────────┘   └──────────┘   └─────────┘ │     └──────────────┘
                       │       │                           │        │
                       │       └───── Single Postgres ─────┘        │
                       │             Transaction                    │
                       └─────────────────────────────────────────────┘
```

1. **Accepts** an event from a broker (NATS JetStream) with a tenant ID, event type, partition key, and JSON payload
2. **Deduplicates** via transactional inbox (unique constraint on tenant_id + event_id)
3. **Finds** the active rule revision for that event type and tenant scope
4. **Evaluates** predicates against event data, selects matching rules
5. **Records** the decision and outbox effects in one atomic PostgreSQL transaction
6. **An async publisher** delivers effects to external destinations with retry and idempotency

## Core Guarantees

| Guarantee | Mechanism |
|-----------|-----------|
| **Exactly-once execution** | Transactional inbox with unique constraint + broker ACK after commit |
| **Exactly-once effect delivery** | Deterministic effect IDs (sha256 of execution+destination+name+payload) as idempotency keys |
| **Per-key ordering** | Virtual shards (4096) with fencing tokens, partition_key routes to shard |
| **Auditability** | Immutable decision hash, pinned rule revision, execution trace ID |
| **Deterministic evaluation** | Pure evaluator: no I/O, no clock, no randomness, no external calls |

## Architecture Overview

FlowRule follows **hexagonal (ports and adapters)** architecture:

- **Domain** (`internal/domain`): Entities, value objects, state machines. Zero dependencies.
- **Rules** (`internal/rules`): Compiler with validation limits, pure evaluator.
- **Services** (`internal/services`): Use cases orchestrating domain logic within transactions.
- **Ports** (`internal/ports`): Interfaces only (broker, repositories, clock).
- **Adapters** (`internal/adapters`): SQL (PostgreSQL), NATS JetStream, in-memory (testing), effects (HTTP/fake).
- **Commands** (`cmd/api`, `cmd/worker`): HTTP API for rule deployment, worker for event processing.

```
cmd/api ──► services/rules ──► ports ──► domain
    │
    └──► sql adapters ──► ports (implementations)

cmd/worker ──► services/events, services/effects ──► ports ──► domain
    │
    └──► nats, sql, effects adapters ──► ports (implementations)
```

## Quick Start

### Prerequisites

- Go 1.21+
- PostgreSQL 14+
- NATS JetStream

### Run Locally

```bash
# Start infrastructure
docker run -d --name flowrule-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
docker run -d --name flowrule-nats -p 4222:4222 -p 8222:8222 nats:latest -js

# Set environment
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/flowrule?sslmode=disable"
export NATS_URL="nats://localhost:4222"

# Start services (migrations run automatically)
go run ./cmd/api &      # :8080
go run ./cmd/worker &   # fetches from NATS
```

### Deploy a Rule

```bash
curl -s -X POST localhost:8080/v1/rules/order.created/revisions \
  -H 'Content-Type: application/json' \
  -d '{
    "rule_set": "order.created",
    "revision": 1,
    "mode": "first_match",
    "rules": [
      {
        "id": "high-value",
        "priority": 100,
        "name": "High value order review",
        "when": { "path": "$.total", "op": "gte", "value": 1000 },
        "then": [{
          "emit": {
            "topic": "orders.review",
            "data": { "reason": "high_value", "order_id": "$.id" }
          }
        }]
      },
      {
        "id": "fraud-check",
        "priority": 50,
        "when": { "path": "$.country", "op": "not_in", "value": ["US","CA","UK"] },
        "then": [{
          "emit": {
            "topic": "orders.fraud-review",
            "data": { "reason": "unusual_country" }
          }
        }]
      }
    ]
  }'
```

### Publish an Event

```bash
# Publish to NATS (requires nats CLI)
nats pub events.order.created '{"id":"order-4821","total":1500,"country":"US","customer":"c-123"}'
```

### Check Execution

```bash
curl -s localhost:8080/v1/executions/{executionID} | jq
```

## Documentation Index

| File | Covers |
|------|--------|
| [architecture.md](architecture.md) | Project structure, ports, component boundaries, dependency rules, port interfaces, capability matrix, sequence diagrams, component wiring, testing architecture |
| [flow.mmd](flow.mmd) | Small map of the complete system and links to the focused Mermaid diagrams below |
| [01-system-design.mmd](01-system-design.mmd) | Design: hexagonal layers, ports, and adapters |
| [02-dependency-direction.mmd](02-dependency-direction.mmd) | Design: allowed import direction between layers |
| [03-api-surface.mmd](03-api-surface.mmd) | Technical: HTTP routes, handlers, services, and repositories |
| [04-rule-structure.mmd](04-rule-structure.mmd) | Technical: rule JSON shape, compilation, predicates, and actions |
| [05-rule-lifecycle.mmd](05-rule-lifecycle.mmd) | Scenario: deploy, compile, save, and activate a rule revision |
| [06-rule-evaluation-modes.mmd](06-rule-evaluation-modes.mmd) | Technical: `first_match` versus `all_matches` evaluation modes |
| [07-evaluation-algorithm.mmd](07-evaluation-algorithm.mmd) | Technical: deterministic evaluator control flow |
| [08-event-processing.mmd](08-event-processing.mmd) | Scenario: consume, deduplicate, evaluate, commit, and acknowledge an event |
| [09-worker-runtime.mmd](09-worker-runtime.mmd) | Technical: worker fetch loop and concurrent effect publishing loop |
| [10-effect-delivery.mmd](10-effect-delivery.mmd) | Scenario: claim outbox effects, deliver, retry, or quarantine them |
| [11-outbox-claim-protocol.mmd](11-outbox-claim-protocol.mmd) | Technical: concurrent publishers and `SKIP LOCKED` outbox claims |
| [12-execution-lifecycle.mmd](12-execution-lifecycle.mmd) | Technical: execution state transitions |
| [13-outbox-lifecycle.mmd](13-outbox-lifecycle.mmd) | Technical: outbox-effect state transitions and claim expiry |
| [14-state-transitions.mmd](14-state-transitions.mmd) | Technical: combined inbox, execution, and outbox state machines |
| [15-error-classification.mmd](15-error-classification.mmd) | Technical: permanent versus transient error handling paths |
| [16-exactly-once-recovery.mmd](16-exactly-once-recovery.mmd) | Technical: redelivery, commit recovery, and idempotent effects |
| [17-sharding-ordering.mmd](17-sharding-ordering.mmd) | Technical: partition keys, virtual shards, leases, and ordering |
| [18-scaling-and-ordering.mmd](18-scaling-and-ordering.mmd) | Technical: worker scaling and current shard-enforcement roadmap constraint |
| [19-data-model.mmd](19-data-model.mmd) | Design: PostgreSQL tables and their core relationships |
| [20-deployment-topology.mmd](20-deployment-topology.mmd) | Operations: processes, infrastructure, ports, and configuration |
| [21-testing-architecture.mmd](21-testing-architecture.mmd) | Technical: unit, contract, and integration test layers |
| [22-business-order-review.mmd](22-business-order-review.mmd) | Business example: route a high-value order from a rule to human review |
| [23-operations-troubleshooting.mmd](23-operations-troubleshooting.mmd) | Operations: investigate stalled events, delivery failures, and latency |
| [24-single-service-target.mmd](24-single-service-target.mmd) | Target architecture: one deployable, internally modular business-event runtime |
| [25-rule-chain-routing.mmd](25-rule-chain-routing.mmd) | Target flow: chained events and independent partition-key routing |
| [26-execution-modes.mmd](26-execution-modes.mmd) | Target design: immediate, fetch, accumulation, scheduled, and outbox batch modes |
| [27-order-flow-example.mmd](27-order-flow-example.mmd) | Business example: one order event fans out to same-key and different-key flows |
| [future-architecture.md](future-architecture.md) | Target architecture, current-versus-proposed capability boundaries, and delivery phases |
| [design.md](design.md) | HLD, LLD, data model, transaction algorithm, consistency model, bounded contexts, state machines, concurrency patterns, failure handling, partitioning |
| [rules.md](rules.md) | Rule shape, compilation, evaluation, operators, contracts, limits, determinism, error handling, best practices, testing, deployment lifecycle |
| [operations.md](operations.md) | Deployment, failure scenarios, partitioning, scaling, metrics, production checklist, troubleshooting, runbooks, capacity planning |
| [roadmap.md](roadmap.md) | Implementation status, gaps, target plan, what we will not build, technical decisions, release criteria |

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/rules/{ruleSet}/revisions` | Deploy and activate a new rule revision |
| POST | `/v1/rules/{ruleSet}/revisions/{revision}/activate` | Activate existing revision (returns 501 Not Implemented) |
| GET | `/v1/executions/{executionID}` | Get execution details with decision |
| GET | `/health` | Health check |

### API Request/Response Examples

#### Deploy Rule Revision

**Request:**
```http
POST /v1/rules/order.created/revisions
Content-Type: application/json

{
  "rule_set": "order.created",
  "revision": 1,
  "mode": "first_match",
  "input_contract": {
    "name": "OrderEvent",
    "version": "1.0",
    "schema_hash": "a1b2c3d4"
  },
  "rules": [
    {
      "id": "high-value",
      "priority": 100,
      "name": "High value order review",
      "when": { "path": "$.total", "op": "gte", "value": 1000 },
      "then": [{
        "emit": {
          "topic": "orders.review",
          "data": { "reason": "high_value", "order_id": "$.id" }
        }
      }]
    }
  ]
}
```

**Response (200 OK):**
```json
{
  "tenant_scope": "default",
  "rule_set": "order.created",
  "revision": 1,
  "version": 1,
  "actor": "api",
  "activated_at": "2026-01-15T10:30:00Z"
}
```

#### Get Execution

**Request:**
```http
GET /v1/executions/exec-abc123
```

**Response (200 OK):**
```json
{
  "id": "exec-abc123",
  "event_id": "evt-xyz789",
  "tenant_id": "default",
  "rule_set": "order.created",
  "revision": 1,
  "decision_hash": "sha256...",
  "status": "completed",
  "error": null,
  "trace_id": null,
  "created_at": "2026-01-15T10:30:05Z",
  "completed_at": "2026-01-15T10:30:05Z"
}
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| DATABASE_URL | `postgres://postgres:postgres@localhost:5432/flowrule?sslmode=disable` | PostgreSQL connection string |
| NATS_URL | `nats://localhost:4222` | NATS server URL |
| MIGRATIONS_DIR | `migrations` | Path to SQL migrations |
| API_LISTEN | `:8080` | HTTP server address |

The following are configured in code (cmd/worker/main.go), not via environment variables:

| Setting | Value | Description |
|---------|-------|-------------|
| STREAM | `flowrule` | JetStream stream name |
| CONSUMER | `flowrule-worker` | JetStream consumer name |
| SUBJECTS | `events.>` | Subject filter for event consumption |
| ACK_WAIT | `30s` | NATS ack wait timeout |
| MAX_DELIVER | `10` | Max delivery attempts before NATS requeues |
| PUBLISH_INTERVAL | `5s` | Effect publisher batch interval |
| PUBLISH_BATCH_SIZE | `10` | Effects per publish batch |

### Compiler Limits (Configurable via rules.DefaultLimits())

| Limit | Default | Description |
|-------|---------|-------------|
| MAX_RULES_PER_SET | `100` | Maximum rules in a single rule set |
| MAX_PREDICATES | `200` | Total predicates across all rules |
| MAX_NESTING_DEPTH | `10` | Maximum nesting depth for composite predicates |
| MAX_ACTIONS_PER_SET | `10` | Total actions across all rules |
| MAX_PAYLOAD_BYTES | `262144` | 256KB compiler limit |
| MAX_RULE_ID_LEN | `128` | Maximum rule ID length |

## Testing

### Unit Tests

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run specific package
go test ./internal/rules/...
```

### Integration Tests

```bash
# Start test infrastructure
docker compose -f docker-compose.test.yml up -d

# Run integration tests
go test -tags=integration ./tests/integration/...
```

### Contract Tests

```bash
# Run adapter contract tests (SQL, NATS, memory)
go test ./tests/contract/...
```

### Example Test: Rule Evaluation

```go
func TestHighValueOrderRule(t *testing.T) {
    compiler := rules.NewCompiler(rules.DefaultLimits())
    evaluator := rules.NewEvaluator()

    source := json.RawMessage(`{
        "rule_set": "order.created",
        "revision": 1,
        "mode": "first_match",
        "rules": [{
            "id": "high-value",
            "priority": 100,
            "when": { "path": "$.total", "op": "gte", "value": 1000 },
            "then": [{ "emit": { "topic": "review", "data": { "order_id": "$.id" } } }]
        }]
    }`)

    revision, err := compiler.Compile(source)
    require.NoError(t, err)

    event := &domain.EventEnvelope{
        ID:           "evt-1",
        Type:         "order.created",
        TenantID:     "tenant-1",
        PartitionKey: "order-1",
        OccurredAt:   time.Now(),
        Data:         json.RawMessage(`{"id": "order-1", "total": 1500}`),
    }

    decision, err := evaluator.Evaluate(revision, event, nil)
    require.NoError(t, err)
    assert.Len(t, decision.MatchedRules, 1)
    assert.Equal(t, "high-value", decision.MatchedRules[0])
    assert.Len(t, decision.Effects, 1)
}
```

## Advanced Examples

### Composite Predicates

```json
{
  "id": "complex-rule",
  "priority": 50,
  "when": {
    "all": [
      { "path": "$.customer.tier", "op": "eq", "value": "premium" },
      {
        "any": [
          { "path": "$.total", "op": "gte", "value": 5000 },
          { "path": "$.items", "op": "exists" }
        ]
      },
      {
        "not": {
          "path": "$.flags", "op": "contains", "value": "test"
        }
      }
    ]
  },
  "then": [
    { "emit": { "topic": "vip.orders", "data": { "order_id": "$.id" } } },
    { "command": { "destination": "workflow/notify", "name": "vip_alert", "data": { "customer_id": "$.customer.id" } } }
  ]
}
```

### Using `otherwise` for Default Handling (first_match mode only)

```json
{
  "rules": [
    { "id": "high-value", "priority": 100, "when": { "path": "$.total", "op": "gte", "value": 1000 }, "then": [...] },
    { "id": "standard", "priority": 50, "when": { "path": "$.total", "op": "gte", "value": 100 }, "then": [...] },
    { "id": "default", "priority": 1, "when": null, "then": [
      { "emit": { "topic": "orders.default", "data": { "order_id": "$.id" } } }
    ]}
  ]
}
```

### Input Contract Validation

```json
{
  "rule_set": "order.created",
  "revision": 2,
  "mode": "first_match",
  "input_contract": {
    "name": "OrderEvent",
    "version": "1.0",
    "schema_hash": "a1b2c3d4"
  },
  "rules": [...]
}
```

This prevents activating rules that reference non-existent fields like `$.nonexistent.field`.

## License

Proprietary.
