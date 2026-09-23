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
        "when": { "path": "$.country", "op": "nin", "value": ["US","CA","UK"] },
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
| [architecture.md](architecture.md) | Project structure, ports, component boundaries, dependency rules, port interfaces, capability matrix, sequence diagrams |
| [design.md](design.md) | HLD, LLD, data model, transaction algorithm, consistency model, bounded contexts, state machines, concurrency patterns |
| [rules.md](rules.md) | Rule shape, compilation, evaluation, operators, contracts, limits, determinism, error handling, best practices, testing |
| [operations.md](operations.md) | Deployment, failure scenarios, partitioning, scaling, metrics, production checklist, troubleshooting |
| [roadmap.md](roadmap.md) | Implementation status, gaps, target plan, what we will not build, technical decisions |

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/rules/{ruleSet}/revisions` | Deploy and activate a new rule revision |
| POST | `/v1/rules/{ruleSet}/revisions/{revision}/activate` | Activate existing revision (not implemented) |
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
  "trace_id": "trace-123",
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
| STREAM | `flowrule` | JetStream stream name |
| CONSUMER | `flowrule-worker` | JetStream consumer name |
| SUBJECTS | `events.>` | Subject filter for event consumption |
| ACK_WAIT | `30s` | NATS ack wait timeout |
| MAX_DELIVER | `10` | Max delivery attempts before NATS requeues |
| PUBLISH_INTERVAL | `5s` | Effect publisher batch interval |
| PUBLISH_BATCH_SIZE | `10` | Effects per publish batch |

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

## License

Proprietary.