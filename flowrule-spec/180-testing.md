# Testing Specification

## 1. Testing Philosophy

- **Deterministic by design**: Pure VM + injected interfaces = predictable tests
- **Spec-level tests**: Same test suite runs against all implementations
- **No mocks, stubs are fine**: Fake implementations of Caller, Breaker, Credit
- **Property-based**: Fuzz the parser and bytecode decoder

## 2. Test Levels

### 2.1 Unit Tests
| Package | Coverage Target | Key Tests |
|---------|----------------|-----------|
| DSL (Lexer) | 100% lines | Token recognition, error cases, edge inputs |
| DSL (Parser) | 100% lines | Structural rules, gate parsing, error cases |
| DSL (Optimizer) | 100% lines | Dead code elimination, hoisting, merging |
| DSL (Compiler) | 100% lines | Target resolution, validation |
| Memory | 100% lines | Arena bounds, slab allocation, interning |
| VM | 95%+ lines | Each opcode in isolation, combined flows |
| Bytecode | 95%+ lines | Encode/decode roundtrip, bounds checking |
| Engine | 90%+ lines | Rule matching, hot reload, shutdown |
| Transports | 90%+ lines | HTTP/gRPC call, timeout, error handling |
| Plugins | 90%+ lines | WASM invoke, timeout, passthrough |

### 2.2 Integration Tests
| Scenario | Scope |
|----------|-------|
| DSL → Compile → Bytecode → VM Execute | Full pipeline |
| Bytecode encode → file → decode roundtrip | Serialization |
| Gate → Next → Fallback chain | Error recovery |
| Parallel → Collect → Emit | Concurrency |
| Hot reload rule table | Live update |
| Circuit breaker open → half-open → closed | State machine |
| Credit exhaustion → backpressure | Flow control |

### 2.3 End-to-End Tests
| Scenario | Components |
|----------|------------|
| HTTP ingress → rule match → VM → HTTP egress | Full stack |
| gRPC ingress → rule match → VM → gRPC egress | Full stack |
| WASM plugin in GATE position | Plugin integration |
| DLQ write → read → replay | Storage integration |

### 2.4 Benchmark Tests
| Benchmark | Target | Measurement |
|-----------|--------|-------------|
| Lex throughput | >10M tokens/s | ns/op, B/op, allocs/op |
| Parse throughput | >5M instr/s | ns/op, B/op, allocs/op |
| Compile throughput | >2M plans/s | ns/op, B/op, allocs/op |
| VM dispatch (NEXT) | <100ns/op | ns/op, B/op, allocs/op |
| VM dispatch (GATE) | <200ns/op | ns/op, B/op, allocs/op |
| Arena Alloc | <10ns/op | B/op, allocs/op (target 0) |
| Gate Eval | <100ns/op | B/op, allocs/op (target 0) |

## 3. Test Infrastructure

### 3.1 Fake Implementations
```go
// FakeCaller returns configured responses
type FakeCaller struct {
    responses map[string]fakeResponse
}

// FakeBreaker simulates circuit state
type FakeBreaker struct {
    allow bool
}

// FakeCredit simulates credit balance
type FakeCredit struct {
    balance map[string]int
}
```

### 3.2 Test Fixtures
```
testdata/
  rules/
    valid.yaml
    invalid.yaml
  bytecode/
    simple.flow
    parallel.flow
    invalid.flow
  plugins/
    identity.wasm
    transform.wasm
```

### 3.3 Cross-Implementation Tests
Test vectors stored in spec repository:
```
flowrule-spec/
  tests/
    vectors/
      simple-next.json       // input → expected output
      gate-pass.json
      parallel-collect.json
      retry-fallback.json
```

Each implementation loads the same vectors and asserts identical results.

## 4. Fuzz Testing

| Target | Fuzzer |
|--------|--------|
| DSL Lexer | Random strings |
| DSL Parser | Random token sequences |
| DSL Optimizer | Random valid programs |
| Bytecode Decoder | Random byte sequences |
| Bytecode Encoder | Random modules |
| VM Execute | Random bytecode + messages |

## 5. CI Requirements

- All tests pass on every commit
- Benchmarks compared against baseline (regression detection)
- Fuzz tests run nightly
- Coverage report generated
- Cross-implementation test vectors validated
