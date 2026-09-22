# Roadmap

## Implementation status

### Phase 0 — Contracts and proof (DONE)

- [x] Event envelope and rule JSON schema
- [x] Compiler with validation limits
- [x] Pure evaluator with first_match and all_matches
- [x] Deterministic effect IDs
- [x] Decision hash

### Phase 1 — Single-process vertical slice (DONE)

- [x] PostgreSQL schema (7 tables, migrations)
- [x] Transactional inbox/outbox in one Postgres transaction
- [x] Broker ACK only after commit
- [x] NATS JetStream consumer and publisher
- [x] Worker fetch/evaluate/ack loop
- [x] API for rule deployment
- [x] Outbox claim with FOR UPDATE SKIP LOCKED
- [x] HTTP destination adapter
- [x] Graceful shutdown with in-flight drain
- [x] MaxDeliver = 10

### Phase 1.5 — Architecture and correctness (DONE)

- [x] Service layer split (rules, events, effects)
- [x] Port interfaces (EventProcessor, EffectPublisher, RuleCompiler, RuleEvaluator)
- [x] Domain state machines (Execution, OutboxEffect, InboxEntry)
- [x] Contract-aware rule revisions
- [x] Rule explanations (explainability)
- [x] Rule metadata (name, description, tags)
- [x] SystemClock in domain package
- [x] Adapter-local Querier, pure ports.Tx
- [x] Contract test suites wired
- [x] Dead code cleanup

### Phase 2 — Production-shaped distribution (TODO)

- [ ] Shard ownership via ShardLease with fencing tokens
- [ ] Worker subject routing by virtual shard
- [ ] Bounded concurrency per shard
- [ ] Metrics and structured logging
- [ ] Replay API
- [ ] Rule-list/read endpoint
- [ ] Authentication and authorization

### Phase 3 — Operational hardening (TODO)

- [ ] Load, failure, and security test suites
- [ ] Retention policies and payload archival
- [ ] Hot-key detection and mitigation
- [ ] Canary revision activation with rollback
- [ ] Monitoring dashboards and alerts

### Phase 4 — Kafka adapter (TODO)

- [ ] Kafka consumer adapter
- [ ] Kafka publisher adapter
- [ ] Transport abstraction validation

## Known gaps

| Gap | Status | Fix order |
|-----|--------|-----------|
| Shard ownership not enforced | Open | 1 |
| No real destination adapters | Open | 2 |
| In-memory quarantine loses data on restart | Open | 3 |
| Worker processes sequentially | Open | 4 |
| No authentication | Open | 5 |
| No replay API | Open | 6 |
| No metrics/observability | Open | 7 |

## Target success criteria

1. Duplicate effect rate = 0 (idempotent delivery verified)
2. Per-key ordering maintained across worker restarts
3. Decision p99 < 10ms for 1KB events
4. Backlog recovery within 60s after worker restart
5. Replay produces identical execution + effect set
6. Operational surface: start, stop, deploy, rollback, quarantine inspect

## What we will not build

- Raft/gossip consensus (Postgres and NATS handle this)
- Arbitrary loops, timers, or compensation in rules
- Global event ordering
- Cross-database 2PC
- Sync request/reply as a rule primitive
