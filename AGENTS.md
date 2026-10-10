# AGENTS.md — FlowRule Development Guide

## Quick Start

```bash
# Install tools
make tools

# Start infrastructure (Postgres, NATS, Redis)
docker-compose up -d

# Run migrations
make migrate-up

# Run worker (dev mode)
make run-worker
# or: go run ./cmd/worker

# Run API (dev mode)
make run-api
# or: go run ./cmd/api

# Run tests
make test           # unit + integration
make test-unit      # unit only (./internal/...)
make test-integration # integration only (-tags=integration ./tests/...)
```

## Architecture (Hexagonal)

```
cmd/api, cmd/worker ──► internal/services ──► internal/ports ◄── internal/adapters
                                                         │
                              internal/domain ◄─────────┘
```

- **domain**: Pure Go, no I/O, entities, value objects, errors
- **ports**: Interfaces only (EventProcessor, EffectSender, repositories, Clock)
- **services**: Use cases (events, effects, rules, batches, workflows, quarantine)
- **adapters**: Implementations (sql/, nats/, mongo/, effects/, http/, memory/)
- **cmd**: Composition roots (api, worker, codegen, migrate)

**Dependency rule**: `cmd → services → ports → domain`; `adapters → ports, domain` (never services)

## Critical Commands

| Task | Command |
|------|---------|
| Build all | `make build` |
| Lint | `make lint` (requires golangci-lint) |
| Vet | `make vet` |
| Format | `make fmt` |
| Generate code | `make generate` (runs `go run ./cmd/codegen generate --all`) |
| Proto gen | `make proto-gen` |
| Migrate up | `make migrate-up` |
| Migrate down | `make migrate-down` |
| Clean | `make clean` |
| CI pipeline | `make ci` (deps → fmt → vet → lint → test-unit → verify-generated) |

## Configuration (Environment Variables)

All config via `internal/config/config.go` — required for production:

```
# Required in production
EFFECT_DESTINATION_URL     # HTTP endpoint for effect delivery (worker fails without it)

# Database
STORAGE_BACKEND            # postgres or mongodb (default: postgres)
DATABASE_URL               # required for postgres
MONGODB_URI, MONGODB_DATABASE  # required for mongodb

# NATS
NATS_URL                   # default: nats://localhost:4222
NATS_STREAM                # default: flowrule
NATS_CONSUMER              # optional

# Worker
NUM_SHARDS                 # default: 4096
WORKER_ID                  # auto-generated if empty
MAX_GLOBAL_IN_FLIGHT       # default: 100
MAX_PER_KEY_QUEUE          # default: 100
KEY_QUEUE_WORKERS          # default: 1
BATCH_MODE                 # none|key|global (default: none)
BATCH_MAX, BATCH_WINDOW_MS
SCHEDULER_POLL_MS          # default: 5000
SCHEDULER_BATCH_SIZE       # default: 100
LEASE_TTL_MS               # default: 30000
LEASE_RENEWAL_INTERVAL_MS  # default: 10000
```

## Test Conventions

- **Unit tests**: `*_test.go` in `internal/...` — run with `make test-unit`
- **Contract tests**: `tests/contract/` — adapter conformance suites (in-memory + SQL + Mongo)
- **Integration tests**: `tests/integration/` — requires `-tags=integration`, spins up test DB
- **E2E tests**: `tests/e2e/` — requires `-tags=e2e`, full infrastructure

Run single test: `go test -v -run TestName ./internal/services/events/...`

## Known Issues / Gotchas

1. **Shard 0 bypasses fencing** — `Process()` validates fencing only when `fencingToken > 0 && shard > 0 && workerID != ""`. Events on shard 0 skip validation entirely.
2. **Dedup fall-through** — If `Inbox.Insert` returns `!inserted` and execution is nil, code continues instead of returning error.
3. **Fencing validated last** — `ValidateFencingToken` runs just before commit; should fail fast with row lock (`FOR SHARE`).
4. **Worker uses `FakeDestination` in dev** — Production requires `EFFECT_DESTINATION_URL` or worker fails at startup.
5. **Transaction leaks pgx via `Context()`** — `ports.Transaction.Context()` smuggles the pgx transaction; repos depend on it.
6. **Publisher double-send risk** — 1-min lease, sequential sends, no bounded concurrency, bookkeeping uses same `ctx` as send (fails on shutdown).
7. **Repo name mismatch** — Module is `github.com/flowrule/flowrule` but repo is `premchandkpc/kafka-simulate-rule`; imports won't work for external users.
8. **Go version** — `go.mod` says 1.26.0; README says 1.21+.

## Key Files to Know

| Purpose | File |
|---------|------|
| Config loading | `internal/config/config.go` |
| Ports (interfaces) | `internal/ports/ports.go` |
| Event processing | `internal/services/events/service.go` |
| Effect publishing | `internal/services/effects/service.go` |
| Worker wiring | `cmd/worker/main.go` |
| API wiring | `cmd/api/main.go` |
| Storage factory | `internal/storage/storage.go` |
| SQL adapters | `internal/adapters/sql/*.go` |
| NATS consumer | `internal/adapters/nats/jetstream.go` |
| Domain types | `internal/domain/*.go` |
| Rules evaluator | `internal/rules/evaluator.go` |

## Migration Workflow

```bash
# Create new migration
# 1. Add SQL file to migrations/ with next number (e.g., 015_new_feature.sql)
# 2. Run: make migrate-up
# 3. Verify: make test-integration
```

## Code Generation

- Contracts in `examples/*/contracts/*.yaml` and `examples/*/rules/*.yaml`
- Run `make generate` or `go run ./cmd/codegen generate --all`
- Output to `/tmp/flowrule-generated` (cleaned by `make clean`)
- Verify generated code compiles: `make verify-generated`

## Debugging Tips

- Worker logs at `LOG_LEVEL=debug` show event processing, shard leases, effect publishing
- NATS monitoring at `http://localhost:8222` (stream/consumer stats)
- Postgres: `docker exec -it flowrule-postgres psql -U postgres -d flowrule`
- Check inbox/outbox tables for stuck entries: `SELECT * FROM inbox WHERE status != 'committed';`

## PR Checklist

- [ ] `make ci` passes locally
- [ ] New adapters have contract tests in `tests/contract/`
- [ ] Migrations are idempotent and reversible
- [ ] No `log.Fatalf` after `defer` (use `run() error` pattern like `cmd/api/main.go`)
- [ ] Production config validated (`EFFECT_DESTINATION_URL` required)