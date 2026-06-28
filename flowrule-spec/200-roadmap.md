# Roadmap

## Milestone 1: Embedded Runtime (v0.1)

**Goal:** Single-process embedded execution engine with YAML configuration.

- [x] DSL lexer, parser, optimizer, compiler
- [x] Memory subsystem (arena, slab, intern)
- [x] Instruction executor (all 12 base opcodes)
- [x] Rule engine with hot reload
- [x] YAML rule loader
- [x] HTTP transport
- [x] Circuit breaker
- [x] Credit-based backpressure
- [x] Dead letter queue
- [x] Prometheus metrics
- [x] OpenTelemetry tracing
- [x] Structured logging
- [x] Integration tests (E2E pipeline)
- [x] Benchmarks (zero alloc hot path)

## Milestone 2: Bytecode VM (v0.2)

**Goal:** Portable bytecode format with multi-implementation VM.

- [x] Bytecode specification (v1)
- [x] Bytecode encode/decode
- [x] Bytecode compiler (DSL → .flow)
- [x] VM executor (all 17 opcodes)
- [x] VM tests (unit + integration)
- [x] CLI (build, validate, inspect, run)
- [x] WASM plugin host (basic)
- [x] Event bus
- [ ] gRPC transport
- [ ] Bytecode checksum verification

## Milestone 3: Plugin Runtime (v0.3)

**Goal:** Full WASM plugin support with language SDKs.

- [ ] WASM plugin ABI finalized
- [ ] Plugin SDK (Rust, TinyGo)
- [ ] Plugin caching and precompilation
- [ ] Plugin marketplace (future)
- [ ] Plugin capability model
- [ ] Plugin test harness

## Milestone 4: Networking (v0.4)

**Goal:** Multi-transport support with streaming.

- [ ] gRPC streaming transport
- [ ] Kafka sink transport
- [ ] NATS transport
- [ ] Transport health checks
- [ ] Circuit-aware routing
- [ ] Multi-protocol ingress

## Milestone 5: Distributed Runtime (v1.0)

**Goal:** Multi-node execution with coordination.

- [ ] Distributed rule store (etcd/consul)
- [ ] Partition-based distribution
- [ ] Node discovery
- [ ] Remote execution API
- [ ] Cross-node trace propagation
- [ ] Cluster metrics aggregation

## Milestone 6: Dashboard (v1.1)

**Goal:** Web UI for monitoring and management.

- [ ] Rule deployment UI
- [ ] Real-time metrics dashboard
- [ ] DLQ browser and replay
- [ ] Plugin management
- [ ] Alert configuration

## Milestone 7: Cloud Platform (v1.2)

**Goal:** Managed FlowRule as a service.

- [ ] Multi-tenant rule isolation
- [ ] API-first rule management
- [ ] Usage-based billing
- [ ] SLA monitoring
- [ ] Audit logging

## Milestone 8: AI Routing (v2.0)

**Goal:** ML-driven routing decisions.

- [ ] Model serving integration
- [ ] A/B testing framework
- [ ] Adaptive circuit breakers
- [ ] Anomaly detection
- [ ] Auto-scaling workers

## Milestone 9: Edge Runtime (v2.1)

**Goal:** Embedded FlowRule on edge devices.

- [ ] Minimal runtime (200KB binary)
- [ ] WASM-only plugins
- [ ] Offline rule caching
- [ ] Sync-on-connect
- [ ] ARM64 support

## Versioning Strategy

| Component | Version Scheme | Breaking Changes |
|-----------|----------------|-----------------|
| Bytecode format | `major.minor` | Major version bump |
| DSL syntax | `major.minor` | Major version bump |
| Go API | Semver | Major version bump |
| Plugin ABI | `major.minor` | Major version bump |
| Transport interface | Semver | Major version bump |

## Current Status

```
v0.1 ──────── v0.2 ──────── v0.3 ──────── v1.0
│              │              │              │
├ DSL Core     ├ Bytecode VM  ├ Plugins      ├ Distributed
├ Executor     ├ WASM host    ├ SDKs         ├ Dashboard
├ Engine       ├ CLI          ├ gRPC         ├ Cloud
├ Transports   ├ Events       ├ Kafka        └ ...
├ Reliability  └ Benchmarks   └ ...
└ Observability

▲ We are here (v0.2)
```
