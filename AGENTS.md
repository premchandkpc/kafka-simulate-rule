# AGENTS.md — FlowRule Engineering & Agent Operating Manual

> Audience: any engineer or AI agent (single or multi-agent) changing this repo.
> Rule zero: **correctness of exactly-once *effects* beats features, speed, and style.**
> Items marked `[VERIFY]` were inferred from docs and must be confirmed in code before you rely on them.

---

## 0. TL;DR (read this if you read nothing else)

1. Hexagonal: `cmd → services → ports → domain`; `adapters → ports, domain`. **Never** `services → adapters`, never `domain → anything`.
2. Every state change for one event happens in **one DB transaction**: inbox + decision + outbox. Broker ACK only **after** commit.
3. Effects are **at-least-once + idempotency key** = *effectively-once*. Never write "exactly-once delivery" in code, docs, or commits. Say "exactly-once execution, idempotent effects".
4. Fencing fails **closed**. Shard `0` is a valid shard. Token `<= 0` is invalid, not "skip".
5. No change is done until `make ci` is green **and** you have shown evidence (test output), not claims.
6. Fix order is in §12. Do not build new features while P0 items are open.

---

## 1. Quick Start

```bash
make tools                  # install dev tools
docker-compose up -d        # Postgres, NATS (JetStream), Redis
make migrate-up
make run-worker             # or: go run ./cmd/worker
make run-api                # or: go run ./cmd/api
make test                   # unit + integration
make test-unit              # ./internal/...
make test-integration       # -tags=integration ./tests/...
```

Single test: `go test -race -count=1 -v -run TestName ./internal/services/events/...`

## 2. Command Reference

| Task | Command |
|------|---------|
| Build all | `make build` |
| Lint | `make lint` (needs golangci-lint) |
| Vet / Format | `make vet` / `make fmt` |
| Generate code | `make generate` (`go run ./cmd/codegen generate --all`) |
| Proto gen | `make proto-gen` |
| Migrate up / down | `make migrate-up` / `make migrate-down` |
| Clean | `make clean` |
| **CI pipeline** | `make ci` (deps → fmt → vet → lint → test-unit → verify-generated) |
| Race tests (required before PR) | `go test -race -count=1 ./...` |
| Integration | `go test -race -count=1 -tags=integration ./tests/integration/...` |
| E2E | `go test -count=1 -tags=e2e ./tests/e2e/...` |

`make ci` is the **minimum** gate. Concurrency or SQL changes also require `-race` and integration (see §9).

---

## 3. Architecture (Hexagonal) — Source of Truth

```
cmd/api, cmd/worker ──► internal/services ──► internal/ports ◄── internal/adapters
                                                     │
                          internal/domain ◄──────────┘
```

| Layer | Contains | May import | Must NOT import |
|-------|----------|-----------|-----------------|
| `domain` | entities, value objects, state machines, domain errors, `SystemClock` | stdlib only | everything else |
| `ports` | interfaces + command/DTO structs | `domain` | `adapters`, `services`, `pgx`, `nats`, `mongo`, `redis` |
| `rules` | compiler, **pure** evaluator | `domain` | any I/O, clock, rand, network |
| `services` | use cases: events, effects, rules, batches, workflows, quarantine | `ports`, `domain`, `rules` (via ports) | `adapters`, drivers |
| `adapters` | sql/, nats/, mongo/, effects/, http/, memory/ | `ports`, `domain` | `services`, other adapters |
| `cmd` | composition roots: api, worker, codegen, migrate | everything | business logic |

Also present (not in the original docs; classify before touching) `[VERIFY]`:
`internal/storage` (backend factory), `internal/application` (worker loop — this is a **driving adapter**, not application core), `internal/runtime/{shard,scheduler}`, `internal/observability`, `internal/config`.

### 3.1 Driving vs driven ports
- **Driving (inbound)**: what the outside calls: `EventProcessor`, `EffectPublisher`, rule management.
- **Driven (outbound)**: what the core needs: repositories, `EffectSender`, `Clock`, `UnitOfWork`, `BrokerConsumer`.
- Target layout: `ports/driving.go`, `ports/driven.go`. Do not add to a single god file.

### 3.2 Mechanical enforcement (add if missing)
Add to `.golangci.yml` so the rule is a failing build, not a wiki page:

```yaml
linters:
  enable: [depguard, errcheck, govet, staticcheck, gosec, errorlint, contextcheck, bodyclose, gocritic, revive]
linters-settings:
  depguard:
    rules:
      domain-pure:
        files: ["**/internal/domain/**"]
        deny: [{pkg: "github.com/flowrule/flowrule/internal", desc: "domain imports stdlib only"}]
      core-no-drivers:
        files: ["**/internal/services/**", "**/internal/ports/**"]
        deny:
          - {pkg: "github.com/jackc/pgx", desc: "driver in core"}
          - {pkg: "github.com/nats-io", desc: "driver in core"}
          - {pkg: "go.mongodb.org", desc: "driver in core"}
          - {pkg: "github.com/redis", desc: "driver in core"}
          - {pkg: "github.com/flowrule/flowrule/internal/adapters", desc: "core must not see adapters"}
```

---

## 4. Non-Negotiable Invariants

Violating any of these is a **blocker**, regardless of test status. Each needs a test that fails if broken.

| # | Invariant | Where enforced | Required test |
|---|-----------|----------------|---------------|
| I1 | Inbox insert, execution, outbox rows, inbox mark-committed commit **atomically** | `services/events` + `UnitOfWork` | kill tx mid-way → no partial rows |
| I2 | Broker ACK only after successful commit; Nak/Retry never after commit | worker loop | crash-before-ack → redelivery yields **same** execution |
| I3 | Duplicate `(tenant_id, event_id)` never creates a 2nd execution | unique constraint + service | N concurrent identical events → exactly 1 execution |
| I4 | Evaluator is pure: no I/O, clock, randomness, map-iteration order dependence | `internal/rules` | same input ×1000 → identical `Decision.Hash`; fuzz |
| I5 | Effect ID deterministic: sha256(execution + destination + name + payload) | `domain`/`rules` | golden-hash test |
| I6 | Fencing validated **first** inside the tx, with row lock; **fail closed**; shard `0` valid | `services/events` | stale token → reject; token 0 → reject; shard 0 → validated |
| I7 | Outbox state changes are **claimant-fenced** (a stale publisher cannot overwrite) | `adapters/*/outbox` | expired-lease publisher cannot MarkDelivered/Retry/Quarantine |
| I8 | Quarantine entry + outbox status change are **one** transaction | `OutboxRepository` | crash between → no orphan |
| I9 | Bookkeeping (settle/ack) uses a context that **survives shutdown** | publisher | SIGTERM mid-send → effect state still settled |
| I10 | Core imports no driver; adapters pass the contract suite | depguard + `tests/contract` | CI |
| I11 | Tenant isolation: every query is scoped by `tenant_id`/`tenant_scope` | adapters | cross-tenant read test |
| I12 | Secrets never logged; payloads logged only by hash/ID | all | log-scrub test `[VERIFY]` |

---

## 5. Agent Operating Protocol

### 5.1 Single-agent loop (default)
```
1. ORIENT   read this file, §4 invariants, the code you will touch, its tests. Run `git status` + `make ci` for a baseline.
2. CLAIM    state the goal, the files you will touch, and the invariants at risk (one short paragraph).
3. RED      write the failing test first (unit → contract → integration as needed).
4. GREEN    smallest change that passes. No drive-by refactors.
5. VERIFY   make ci; go test -race -count=1 on touched pkgs; integration if SQL/concurrency/broker touched.
6. REVIEW   re-read your diff as a hostile reviewer: check §4, §7, §8.
7. REPORT   use the Evidence Format (§5.4). Never claim "should work".
```

### 5.2 Multi-agent protocol (swarm)
Use when work spans ≥ 3 packages or ≥ 2 independent concerns. **One orchestrator, N lane workers, one independent reviewer.**

**Lanes (single owner per path per wave):**

| Lane | Owns (write) | Reads only |
|------|--------------|-----------|
| A — Contracts | `internal/ports/**`, `internal/domain/**` | everything |
| B — Use cases | `internal/services/**`, `internal/rules/**` | ports, domain |
| C — Adapters | `internal/adapters/**`, `internal/storage/**`, `migrations/**` | ports, domain |
| D — Runtime | `cmd/**`, `internal/application/**`, `internal/runtime/**`, `internal/config/**` | all |
| E — Verification | `tests/**`, chaos scripts, benchmarks | all |
| F — Docs/Obs | `docs/**`, `AGENTS.md`, `internal/observability/**` | all |

**Rules**
1. **Contract-first.** Lane A lands port/domain changes **first** and alone. Other lanes branch from that commit. No lane edits another lane's paths; request the change via a handoff note.
2. **Isolation.** One git worktree per agent: `git worktree add ../fr-<lane>-<topic> -b agent/<lane>/<topic>`.
3. **Waves.** Orchestrator defines waves (§12 has a ready plan). Next wave starts only when the previous wave's gates are green on `main`.
4. **Merge order**: A → C → B → D → E → F. Rebase, never merge-commit agent branches.
5. **Independent review.** The reviewing agent must not be the author. Reviewer runs the checks itself; does not trust the author's report.
6. **Conflict protocol.** Two agents need the same file → stop, orchestrator serializes. Never "resolve" by overwriting.
7. **Budget.** Max 2 failed fix attempts on the same failure → escalate with the failing output (§5.5).

### 5.3 Handoff note (between lanes)
```
FROM: lane-B  TO: lane-C  WAVE: 2
NEED:   LeaseRepository.AssertHeld(ctx, ShardLease) error  (SELECT ... FOR SHARE on lease row)
WHY:    I6 – fencing must be first op in tx and lock the row
CONTRACT: ports/driven.go @ <commit sha>
ACCEPTANCE: contract test TestLeaseAssertHeld passes for sql+mongo+memory
```

### 5.4 Evidence format (every report)
```
CHANGED:   <files>
INVARIANTS TOUCHED: I3, I6
TESTS ADDED: <names>   (each shown to FAIL before the fix: yes/no)
COMMANDS RUN + RESULT:
  make ci                                   → PASS
  go test -race -count=1 ./internal/services/... → PASS
  go test -race -tags=integration ./tests/integration/... → PASS
NOT VERIFIED: <honest list>
RISK / ROLLBACK: <one line>
```
"Not verified" must never be empty by habit. Say what you could not run.

### 5.5 Escalate to the human when
- A change needs a **port signature change** that breaks adapters (needs ADR, §13).
- A migration is destructive, non-reversible, or locks a hot table.
- Two invariants conflict, or a requirement contradicts §4.
- You are blocked after 2 attempts, or tests are flaky and you cannot prove why.
- You would need secrets, production access, or to delete data.

### 5.6 Sub-agent prompt templates
**Implementer**
> You are Lane {X}. Own only {paths}. Goal: {goal}. Invariants at risk: {I#}. Write the failing test first. Do not touch other lanes' files. Run `make ci` and `go test -race` on touched packages. Report in Evidence Format with an honest NOT VERIFIED section.

**Reviewer (hostile)**
> You did not write this code. Check the diff against AGENTS.md §4 (every invariant), §7 (Go rules), §8 (SQL rules). Run the tests yourself. Try to break it: shard 0, token 0, duplicate event, crash after commit before ACK, SIGTERM mid-send, expired lease, cross-tenant. List blockers vs nits. Do not approve without running tests.

**Chaos/Verifier**
> Build and run the failure-injection suite (§9.3). Report duplicates, lost effects, and recovery time with numbers.

---

## 6. Configuration (Environment Variables)

All config via `internal/config/config.go`.

```
# Required in production
EFFECT_DESTINATION_URL     # HTTP endpoint for effect delivery (worker must fail at startup without it)

# Storage
STORAGE_BACKEND            # postgres | mongodb (default: postgres)
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
LOG_LEVEL                  # debug|info|warn|error
```

**Config rules**
- No magic numbers in `main`. `AckWait`, `MaxDeliver`, `HotKeyThreshold`, `HotKeyCheckInterval`, publish interval/batch, send timeout, lease TTL for outbox claims all come from config with validated defaults. `[VERIFY]` several are currently hardcoded.
- **Timing constraint (enforce in `Config.Validate()`):** `LEASE_RENEWAL_INTERVAL_MS * 3 <= LEASE_TTL_MS`, and `outbox_send_timeout * ceil(batch / max_in_flight) < outbox_claim_ttl`.
- Production mode must refuse `FakeDestination`. Fake is allowed only when `ENV=dev|test`.

---

## 7. Go Engineering Rules

**Structure & errors**
- `main` is `func main(){ os.Exit(run()) }` with `run() error`. **No `log.Fatalf` after `defer`** (it skips deferred cleanup).
- Wrap with `%w` and context: `fmt.Errorf("claim pending: %w", err)`. Compare with `errors.Is/As`. Domain errors live in `domain`; adapters translate driver errors **into** domain errors at the boundary.
- Classify errors: `Permanent` (4xx, validation, no rule) vs `Transient` (timeouts, 5xx, deadlock). Permanent → quarantine immediately; transient → retry with **jittered** backoff. Do not burn retries on permanent errors.
- No `panic` outside `main`/init; no swallowed errors (`_ =` only with a comment why).

**Context & concurrency**
- `ctx` is first param; never stored in structs; never used to smuggle transactions. Use `UnitOfWork.Do(ctx, func(ctx, repos) error)`.
- Every goroutine has an owner, a stop condition, and is waited on (`errgroup`, bounded with `SetLimit`). No fire-and-forget.
- Bookkeeping after work (ack, settle, release lease) uses `context.WithoutCancel(ctx)` + its own timeout.
- Every outbound call has a timeout shorter than the lease/ack window it lives inside.
- Shared state: prefer ownership by one goroutine; otherwise `sync`/atomics with `-race` coverage. No unbounded channels/queues.
- Use `goleak` (`go.uber.org/goleak`) in packages that spawn goroutines.

**Hot-path performance (decision target p99 < 10 ms @ 1 KB event)**
- Avoid per-event allocations in the evaluator: precompile JSONPath, reuse buffers (`sync.Pool`) only with benchmarks proving the win.
- 1 round trip for dedup (`INSERT … ON CONFLICT DO NOTHING`), not Get-then-Insert.
- Batch outbox inserts and `MarkDelivered`; no N+1 writes.
- Every perf claim needs `go test -bench . -benchmem` before/after in the PR.

**Style**
- Small interfaces defined by the consumer. Accept interfaces, return structs. Per-use-case repo sets (e.g. `EventRepos`), not a 10-repo `TxRepos`.
- Type aliases of port types inside services are banned (noise).
- Use the `observability` logger (structured). Stdlib `log` is banned in `internal/`.
- Exported identifiers have doc comments that start with the name.

---

## 8. Data & SQL Rules

- Migrations: `migrations/NNN_name.sql`, next sequential number, **idempotent and reversible** (up + down). Never edit an applied migration.
- Zero-downtime: add nullable columns / `CREATE INDEX CONCURRENTLY`; backfill in batches; no table rewrites or long `ACCESS EXCLUSIVE` locks on `inbox`, `outbox`, `executions`.
- Required constraints: `UNIQUE (tenant_id, event_id)` on inbox; PK on effect ID; FK/indices on `execution_id`, `status+available_at` (outbox claim), `claimed_by+claim_expires_at`.
- Claim query uses `FOR UPDATE SKIP LOCKED` ordered by `available_at`; verify its plan with `EXPLAIN` on realistic row counts.
- Fencing: lease check uses `SELECT … FOR SHARE` (or `FOR UPDATE`) on the lease row **inside** the processing tx.
- Isolation level is stated per transaction in code comments. Default READ COMMITTED; justify anything else.
- Every query is tenant-scoped. Retention/archival for `inbox`, `executions`, `outbox` is a roadmap item: partition by time when row counts demand it.
- Mongo backend `[VERIFY]`: must implement the same semantics (atomic inbox+outbox needs multi-document transactions on a replica set). If it cannot, document it as **unsupported for exactly-once** and block it in prod config.

---

## 9. Testing Strategy

### 9.1 Pyramid
| Layer | Location | Must cover |
|-------|----------|-----------|
| Unit | `internal/**/_test.go` | evaluator, compiler limits, state machines, effect-ID hash, backoff, error classification |
| Property/Fuzz | `internal/rules` | `go test -fuzz` on compiler/evaluator: no panics; determinism |
| Contract | `tests/contract/` | **every** adapter (memory + SQL + Mongo) passes the same suites |
| Integration | `tests/integration/` (`-tags=integration`) | full tx flow, duplicate/race, redelivery, lease expiry |
| E2E | `tests/e2e/` (`-tags=e2e`) | API → NATS → worker → HTTP destination |
| Chaos | `tests/chaos/` (`-tags=chaos`) | §9.3 |
| Bench | `*_bench_test.go` | evaluator p99, claim throughput |

### 9.2 Rules
- Bug fix = failing test first, shown red, then green.
- Tests use `-race -count=1`; no `time.Sleep` for sync (use channels/`Eventually` with timeouts).
- Inject `Clock`; never call `time.Now()` in core code.
- Table-driven tests with edge cases: empty/nil payload, 256 KB limit, nesting depth 10, unicode, tenant mismatch, shard `0` and `4095`.

### 9.3 Failure-injection suite (the proof of the product)
Run against real Postgres + NATS. Assert **zero duplicate executions and zero lost effects**; record recovery time.

1. `kill -9` worker mid-batch, restart, drain.
2. Crash **after commit, before ACK** → redelivery returns same execution.
3. Two workers, same shard, one paused 40 s (GC pause simulation) → stale token rejected.
4. Destination returns 200 after client timeout → duplicate send carries same idempotency key.
5. `SIGTERM` mid-send → state settled, no stuck `claimed` rows.
6. Postgres failover/connection drop mid-tx → clean rollback, redelivery.
7. Poison event (no active rule) → bounded retries → quarantine, not a retry storm.
8. Hot key: 80 % traffic on one `partition_key` → ordering preserved, other keys unaffected.
9. NATS restart → consumer resumes without loss.

Publish the results table in `docs/`; it replaces claims with evidence.

---

## 10. Observability & Operations

- **Logs**: structured, include `tenant_id`, `event_id`, `execution_id`, `shard`, `worker_id`, `trace_id`. Never log payload bodies or secrets.
- **Metrics (Prometheus)** `[VERIFY: roadmap says missing]`: `events_processed_total{result}`, `event_process_seconds`, `inbox_duplicates_total`, `outbox_pending`, `outbox_oldest_age_seconds`, `effect_send_seconds{dest,result}`, `effects_quarantined_total`, `lease_renew_failures_total`, `fencing_rejections_total`, `consumer_lag`, `hot_key_events_total`.
- **Traces (OTel)**: span per event: consume → tx → evaluate → commit → ack; link effect publish spans via execution ID.
- **SLOs (draft)**: decision p99 < 10 ms; backlog recovery < 60 s after worker restart; oldest pending effect < 60 s; duplicate effect rate = 0 at idempotent destinations.
- **Alerts**: `outbox_oldest_age_seconds` high, quarantine rate rising, lease-renew failures, fencing rejections spike, consumer lag growing.
- **Health**: `/health` (liveness) and a readiness check that verifies DB + NATS.

### Debugging
- `LOG_LEVEL=debug` shows event processing, shard leases, effect publishing.
- NATS monitoring: `http://localhost:8222`.
- Postgres: `docker exec -it flowrule-postgres psql -U postgres -d flowrule`
- Stuck inbox: `SELECT * FROM inbox WHERE status != 'committed';`
- Stuck outbox: `SELECT id,status,attempts,claimed_by,claim_expires_at FROM outbox WHERE status IN ('pending','claimed') ORDER BY available_at LIMIT 50;` `[VERIFY column names]`

---

## 11. Security

- API has **no authN/authZ today** (roadmap): treat the API as dev-only; do not expose publicly. Adding authN (mTLS/JWT), per-tenant authZ, and request size limits is P1.
- Validate and bound all inputs (compiler limits: 100 rules, 200 predicates, depth 10, 256 KB). Reject, do not truncate.
- Outbound effects: allowlist destinations, TLS verification on, timeouts on, sign requests / send idempotency key header. Block SSRF to link-local/metadata IPs.
- Secrets only via env/secret manager; `.env.example` has placeholders only. `gosec` and `govulncheck` in CI.
- Dependencies: remove unused modules (`go mod tidy`); review new deps; pin versions.
- Multi-tenancy: tenant ID comes from authenticated context, never from user payload alone.

---

## 12. Known Issues & Fix Plan (priority order)

Status: **Open** unless noted. Do **not** start features while P0 is open.

| ID | Pri | Issue | Invariant | Fix |
|----|-----|-------|-----------|-----|
| F1 | P0 | **Shard 0 bypasses fencing**: guard is `fencingToken > 0 && shard > 0 && workerID != ""`; also fails open on token 0 | I6 | `Lease.Validate()` rejects token ≤ 0; validate for all shards incl. 0 |
| F2 | P0 | **Dedup fall-through**: `!inserted` with nil execution continues and creates a 2nd execution | I3 | return `ErrInboxConflict`; single-RTT `InsertIfAbsent` |
| F3 | P0 | **Fencing validated last**, no row lock | I6 | `Leases.AssertHeld` first in tx with `FOR SHARE` |
| F4 | P0 | **Publisher double-send**: 1-min claim lease vs sequential sends; bookkeeping shares send `ctx`; MarkDelivered failure only logged | I7, I9 | bounded `errgroup`, per-send timeout, `WithoutCancel` bookkeeping, timing validation (§6) |
| F5 | P0 | `ScheduleRetry`/`Quarantine` not claimant-fenced; quarantine entry + outbox update are 2 writes | I7, I8 | pass `claimant` everywhere; `QuarantineWithEntry` single tx |
| F6 | P0 | **Worker wires `FakeDestination`** | — | real HTTP adapter w/ idempotency key; fail startup in prod without `EFFECT_DESTINATION_URL` |
| F7 | P1 | `ports.Transaction` leaks pgx via `Context()`, `SetFencingToken`; docs claim "pure" | I10 | replace with `UnitOfWork.Do`; split `ports` driving/driven; per-use-case repo sets |
| F8 | P1 | `Process(ctx, env, fencingToken, shard, workerID)` leaks infra into use-case signature | — | `ProcessCommand{Event, Lease}` |
| F9 | P1 | Error classification only labels quarantine; permanent errors burn all retries; no jitter | — | classify first; jittered exponential backoff |
| F10 | P1 | `Execution` saved `pending` then `completed` in same tx (dead state) | — | save `completed` directly |
| F11 | P1 | N+1 `MarkDelivered`; sequential sends | perf | batch settle; bounded concurrency |
| F12 | P1 | Hardcoded config in `main` (AckWait, MaxDeliver, hot-key) | — | move to config + validation |
| F13 | P1 | No metrics/tracing wired; stdlib `log` in services | §10 | observability logger + metrics |
| F14 | P1 | No authN/authZ on API | §11 | per-tenant auth |
| F15 | P2 | Module path `github.com/flowrule/flowrule` ≠ repo path; `sdk/go` unimportable | — | align module path or repo name |
| F16 | P2 | `go.mod` Go 1.26.0 vs README 1.21+; unused deps (mongo/redis/yaml) vs docs | — | reconcile; `go mod tidy`; document or remove |
| F17 | P2 | NATS subject `events` vs README `events.>` / `events.order.created` `[VERIFY]` | — | align adapter + README + quick start |
| F18 | P2 | Docs drift: `architecture.md` shows old `Tx`, old method signatures, missing packages | — | regenerate from code; cut to a few accurate diagrams |
| F19 | P2 | Repo hygiene: committed binaries (`worker`, `codegen`) `[VERIFY]`, `test_laya.py`, `.temp/`; "Proprietary" license on public repo | — | `git rm`, `.gitignore`, pick license |
| F20 | P2 | Repo name says Kafka; no Kafka adapter (Phase 4) | — | rename repo or ship Kafka adapter; keep broker behind port |
| F21 | P2 | In-memory quarantine loses data on restart `[VERIFY]` | — | persist quarantine |
| F22 | P3 | Replay API, rule list/read endpoints, retention, canary activation | — | roadmap |

### Multi-agent wave plan for P0/P1
```
Wave 0  (orchestrator)  baseline: make ci + -race + integration green on main; record numbers.
Wave 1  Lane A          F7/F8 contracts: UnitOfWork, ProcessCommand, ShardLease, LeaseRepository.AssertHeld,
                        InsertIfAbsent, QuarantineWithEntry, claimant-fenced Outbox signatures. Land alone.
Wave 2  parallel        Lane C: sql/memory(/mongo) adapters + contract tests for new port methods (F3,F5)
                        Lane E: write failing tests for F1,F2,F3,F4,F5 against the new contracts
Wave 3  parallel        Lane B: events service (F1,F2,F3,F8,F10)   |  Lane B': effects service (F4,F5,F9,F11)
                        Lane D: config validation + real HTTP destination wiring (F6,F12)
Wave 4  Lane E          chaos suite §9.3 + benchmarks; publish results
Wave 5  Lane F          metrics/tracing (F13), docs regenerated (F18), hygiene (F15–F20)
Gate    Reviewer agent  independent hostile review of every wave; orchestrator merges in order A→C→B→D→E→F
```

---

## 13. Decision Records (ADRs)

Create `docs/adr/NNNN-title.md` for: port signature changes, new adapters, schema changes to inbox/outbox/lease, delivery-semantics changes, new dependencies.
Template: **Context → Options (≥2) → Decision → Consequences → Invariants affected → Rollback**.

Standing decisions (do not relitigate without an ADR):
- Postgres is the source of truth for dedup/ordering state; the broker is transport only (at-least-once).
- Pure deterministic evaluator; effects emitted as data, executed later by the publisher.
- Not building: Raft/gossip, loops/timers/compensation inside rules, global ordering, cross-DB 2PC, sync request/reply as a rule primitive.

---

## 14. Code Generation

- Contracts: `examples/*/contracts/*.yaml`; rules: `examples/*/rules/*.yaml`.
- `make generate` or `go run ./cmd/codegen generate --all`; output to `/tmp/flowrule-generated` (cleaned by `make clean`).
- `make verify-generated` must pass; generated code is never hand-edited.

## 15. Migration Workflow

```bash
# 1. Add migrations/NNN_name.sql (next number, with down)
# 2. make migrate-up
# 3. make test-integration
# 4. Check EXPLAIN on any new/changed hot query
```

---

## 16. Git, Commits, PRs

- Branches: `agent/<lane>/<topic>` or `feat|fix|chore/<topic>`. One concern per PR, ≤ ~400 changed lines where possible.
- Commits: Conventional (`fix(events): validate fencing for shard 0`). Body states the invariant and the test.
- Never: force-push `main`, commit binaries/secrets, edit applied migrations, skip hooks, or disable a failing test to go green.
- PR description: Problem → Change → Invariants touched → Evidence (§5.4) → Rollback.

### PR Checklist
- [ ] `make ci` passes locally; `go test -race -count=1` on touched packages
- [ ] Failing test existed before the fix (shown)
- [ ] §4 invariants reviewed; none weakened
- [ ] New/changed adapter methods have **contract tests** for memory + SQL (+ Mongo)
- [ ] Migrations idempotent, reversible, non-blocking
- [ ] No `log.Fatalf` after `defer` (use `run() error` like `cmd/api/main.go`)
- [ ] No new driver import in `services`/`ports`/`domain`
- [ ] Production config validated (`EFFECT_DESTINATION_URL` required; Fake refused)
- [ ] Metrics/logs added for new failure paths; no payload/secret logging
- [ ] Docs updated **from code** if a signature or behavior changed
- [ ] Perf-sensitive change includes before/after benchmark

---

## 17. Key Files

| Purpose | File |
|---------|------|
| Config | `internal/config/config.go` |
| Ports | `internal/ports/ports.go` (split → `driving.go` / `driven.go`) |
| Event processing | `internal/services/events/service.go` |
| Effect publishing | `internal/services/effects/service.go` |
| Worker wiring | `cmd/worker/main.go` |
| API wiring | `cmd/api/main.go` |
| Storage factory | `internal/storage/storage.go` |
| SQL adapters | `internal/adapters/sql/*.go` |
| NATS consumer | `internal/adapters/nats/jetstream.go` |
| Domain types | `internal/domain/*.go` |
| Rules evaluator | `internal/rules/evaluator.go` |

## 18. Glossary

- **Inbox**: dedup table keyed by `(tenant_id, event_id)`.
- **Outbox**: durable queue of effects written in the same tx as the decision.
- **Effect ID**: deterministic hash; used as the destination idempotency key.
- **Fencing token**: monotonically increasing number per shard lease; stale holders are rejected.
- **Virtual shard**: `hash(partition_key) % NUM_SHARDS`; unit of ordering and lease ownership.
- **Quarantine**: parking lot for permanent failures, inspectable and replayable.
- **Effectively-once**: at-least-once delivery + idempotent consumer. This is the honest claim.

## 19. Never Do

1. Claim "exactly-once delivery".
2. Skip or weaken fencing "for tests".
3. Fall through on dedup conflicts.
4. Add I/O, clock, or randomness to the evaluator.
5. Put a driver type in `ports`.
6. Use `ctx` to carry a transaction.
7. Fire-and-forget goroutines.
8. Ship `FakeDestination` in a prod path.
9. Mark work done without evidence.
10. "Fix" a flaky test by adding a sleep.