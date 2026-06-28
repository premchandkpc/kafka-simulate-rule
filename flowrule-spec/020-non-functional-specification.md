# Non-Functional Specification

## 1. Performance Budgets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Cold start (first load) | <10ms | Time from decode to ready |
| Compilation | <5ms | DSL text → bytecode |
| Execution overhead | <50μs | VM dispatch per instruction |
| Memory per message | <1KB | Peak allocation during routing |
| P99 latency (1 hop) | <1ms | Excluding transport call |
| P99 latency (parallel 3) | <3ms | Collect sync overhead |
| Instruction dispatch | <100ns | IP advance + decode + branch |
| Hot reload | <1ms | Atomic table swap |

## 2. Throughput

| Scenario | Target | Conditions |
|----------|--------|------------|
| Single next, noop transport | >1M msg/s | Single thread |
| Gate + next, noop transport | >500K msg/s | Single thread |
| Parallel 3 + collect | >200K msg/s | Single thread |
| Real HTTP transport | >10K msg/s | Per target, p99 <100ms |
| Concurrent messages | >10K in-flight | 8 workers |

## 3. Resource Limits

| Resource | Limit | Behavior |
|----------|-------|----------|
| Plugin execution | 5s timeout | Context cancelled |
| Next call (default) | 30s timeout | Configurable per rule |
| Backoff max | 10s | Exponential cap |
| Buffer count | 1–10000 | Validation enforced |
| Rule count | Unlimited | Memory bound |
| Target per parallel | Unlimited | Memory bound |
| Instruction count | 2^32 - 1 | IP is uint32 |
| Module size | 4GB | Section offset is uint32 |

## 4. Reliability Guarantees

| Guarantee | Level | Notes |
|-----------|-------|-------|
| Message durability | At-least-once | With DLQ enabled |
| Delivery ordering | Per partition | KEY instruction ordering |
| No silent drops | Verified | DROP is explicit |
| Crash recovery | Best effort | In-flight lost on crash |
| Hot reload safety | Atomic | Old rules drain gracefully |

## 5. Security Properties

| Property | Mechanism |
|----------|-----------|
| Plugin sandbox | WASM, no host access by default |
| Transport encryption | TLS 1.3 |
| No code injection | Compiled bytecode is immutable |
| No eval | No runtime string-to-code |
| Bounded resources | Timeouts, credits, capacities |
| No reflection | All dispatch is static |

## 6. Compatibility

| Layer | Contract |
|-------|----------|
| Bytecode format | Backward compatible within major version |
| DSL syntax | Additive only within major version |
| Plugin ABI | Versioned capability negotiation |
| Transport interface | Stable Go interface |
| Event bus | Stable typed event types |
| Metrics | Stable Prometheus metric names |

## 7. Scalability

| Dimension | Approach |
|-----------|----------|
| Vertical | Single process, bounded goroutines |
| Horizontal | Application embeds instance, scales with app |
| Message rate | Worker pool, configurable concurrency |
| Target count | Atomic maps, O(1) lookup |
| Rule count | Atomic pointer swap, O(n) match |

## 8. Observability

| Signal | Implementation |
|--------|----------------|
| Metrics | Prometheus, labels per target/rule |
| Traces | OpenTelemetry, span per hop |
| Logs | Structured (zerolog), level per component |
| Events | Typed bus, pub/sub channels |
| DLQ | Snapshot API, TTL-based cleanup |

## 9. Testability

| Requirement | Approach |
|-------------|----------|
| Deterministic execution | Pure VM, no global state |
| Fake transports | Interface injection |
| Snapshot testing | Bytecode encode/decode roundtrip |
| Chaos testing | Circuit breaker, credit injection |
| Fuzz testing | DSL parser, bytecode decoder |
