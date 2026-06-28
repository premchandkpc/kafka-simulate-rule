# Architecture Specification

## 1. System Context

```
┌─────────────────────────────────────────────────────────┐
│                   Application Process                     │
│                                                           │
│  ┌──────────┐   ┌───────────┐   ┌────────────────────┐   │
│  │  Control  │   │  Runtime  │   │    Transports      │   │
│  │  Plane    │──▶│  Engine   │──▶│                    │   │
│  │           │   │           │   │  HTTP │ gRPC │ ... │   │
│  │ Compiler  │   │  VM       │   │                    │   │
│  │ Validator │   │  Scheduler│   └────────────────────┘   │
│  │ Inspector │   │  Memory   │                            │
│  └──────────┘   │  Plugins  │   ┌────────────────────┐   │
│                  │  Events   │   │    Observability   │   │
│                  │  DLQ      │   │                    │   │
│                  └───────────┘   │ Metrics │ Traces   │   │
│                                  │ Logs    │ Events   │   │
│                                  └────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

## 2. Subsystem Ownership

| Subsystem | Responsibility | Owns |
|-----------|----------------|------|
| Control Plane | Compilation, validation, inspection, deployment | Compiler, CLI, rule loader |
| Runtime | Execution lifecycle, resource management, worker pool | Engine, scheduler, memory |
| VM | Instruction dispatch, bytecode interpretation | Dispatcher, opcodes, constant pool |
| Transports | Protocol adaptation, connection pooling, TLS | HTTP client, gRPC client/server, adapters |
| Plugins | WASM sandbox, module cache, capability management | Plugin runtime, ABI |
| Observability | Metrics, traces, logs, events | Prometheus, OTel, zerolog, event bus |
| Reliability | Retry, circuit breaker, DLQ, backpressure | Retry policies, breakers, DLQ store |
| Storage | Durable state, rule persistence, DLQ persistence | File I/O, Badger, snapshot API |

## 3. Control Plane Architecture

```
┌─────────────┐    ┌──────────┐    ┌──────────┐    ┌───────────┐    ┌─────────────┐
│  Rules File  │───▶│  Loader  │───▶│ Compiler │───▶│  Encoder  │───▶│  .flow file │
│  (YAML)      │    │ (watch)  │    │          │    │           │    │             │
└─────────────┘    └──────────┘    └──────────┘    └───────────┘    └─────────────┘
                       │                                │
                       ▼                                ▼
                 ┌──────────┐                    ┌───────────┐
                 │  Engine  │                    │  Inspector│
                 │ (reload) │                    │ (dump)    │
                 └──────────┘                    └───────────┘
```

### 3.1 Compilation Pipeline
1. **Load**: Read YAML rules file, parse rule structure
2. **Lex**: Tokenize DSL source text
3. **Parse**: Build instruction AST
4. **Optimize**: Apply transformations (dead code elimination, instruction merging)
5. **Compile**: Resolve targets, validate semantics, produce execution plan
6. **Encode**: Serialize execution plan to binary `.flow` format
7. **Verify**: Compute checksum, validate encoding, check bounds

### 3.2 Hot Reload
1. File watcher detects change
2. New rules compiled and validated in isolation
3. On success: atomic pointer swap of rule table
4. On failure: error logged, existing rules unchanged
5. In-flight messages complete with pre-swap rules

## 4. Runtime Architecture

```
┌──────────────────────────────────────────────────┐
│                    Engine                          │
│                                                    │
│  ┌──────────┐  ┌──────────┐  ┌─────────────────┐  │
│  │  Match   │  │ Dispatch │  │  Worker Pool     │  │
│  │  (rules) │──▶│ (VM)    │──▶│  (goroutines)   │  │
│  └──────────┘  └──────────┘  └────────┬────────┘  │
│                                       │            │
│  ┌────────────────────────────────────┐│           │
│  │         Scheduler                  ││           │
│  │  ┌──────────┐  ┌──────────────┐   ││           │
│  │  │ Credits  │  │ Partition    │   ││           │
│  │  │ (backpr.)│  │ (ordering)   │   ││           │
│  │  └──────────┘  └──────────────┘   ││           │
│  └────────────────────────────────────┘│           │
│                                        │           │
│  ┌──────────────────────────────────────┘           │
│  ▼                                                   │
│  ┌──────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  VM      │  │  Transports  │  │  Plugins     │  │
│  │ (instr.) │──▶│  (HTTP/...) │  │  (WASM)      │  │
│  └──────────┘  └──────────────┘  └──────────────┘  │
│                                                    │
└──────────────────────────────────────────────────────┘
```

### 4.1 Message Lifecycle
1. **Ingress**: Message arrives via transport or API call
2. **Match**: Rule table scanned, first matching rule selected
3. **Execute**: VM dispatches rule instructions against message
4. **Egress**: Result delivered, emitted, or dropped
5. **Observe**: Metrics, traces, events emitted at each step
6. **Complete**: Worker released, credits returned, resources freed

### 4.2 Worker Model
- Fixed-size goroutine pool
- Each worker processes one message at a time
- Backpressure via credit system
- Context deadlines govern all external calls
- Structured concurrency: parent context cancels all child operations

## 5. VM Architecture

```
┌──────────────────────────────────────┐
│                VM                      │
│                                       │
│  ┌────────────────────────────────┐   │
│  │         Dispatcher              │   │
│  │  ┌─────┐  ┌──────┐  ┌──────┐  │   │
│  │  │ IP  │──▶│Fetch │──▶│Decode│  │   │
│  │  └─────┘  └──────┘  └──┬───┘  │   │
│  │                         │       │   │
│  │  ┌──────────────────────▼────┐  │   │
│  │  │    Opcode Table           │  │   │
│  │  │  NEXT → call + timeout    │  │   │
│  │  │  GATE → eval + branch     │  │   │
│  │  │  JUMP → ip = arg          │  │   │
│  │  │  ...                      │  │   │
│  │  └───────────────────────────┘  │   │
│  └────────────────────────────────┘   │
│                                       │
│  ┌──────────┐  ┌──────────┐          │
│  │ Constant  │  │ Target   │          │
│  │ Pool      │  │ Lists    │          │
│  └──────────┘  └──────────┘          │
└──────────────────────────────────────┘
```

### 5.1 Dispatch Loop
```
while ip < len(instructions):
    instr = instructions[ip]
    switch instr.opcode:
        case NEXT:    call(cp[instr.url], msg)
        case GATE:    if not eval(field, op, value): ip = skip()
        case JUMP:    ip = instr.target; continue
        ...
    ip++
```

### 5.2 Execution Context
- Message (body, headers, metadata)
- Rule (plan, version, id)
- Transport (caller, emitter)
- Policy (breaker, credit)
- Logger
- Deadline (from context)

## 6. Transport Architecture

```
┌──────────────────────────────────────────────┐
│              Transport Layer                    │
│                                                │
│  ┌──────────┐  ┌──────────┐  ┌─────────────┐  │
│  │ HTTP     │  │ gRPC     │  │ Kafka       │  │
│  │ Client   │  │ Client   │  │ Producer    │  │
│  │ pool     │  │ stream   │  │ (future)    │  │
│  └──────────┘  └──────────┘  └─────────────┘  │
│                                                │
│  ┌────────────────────────────────────────────┐ │
│  │           Adapter Interface                 │ │
│  │  Call(ctx, target, body) → (result, err)   │ │
│  │  Emit(ctx, target, body) → error           │ │
│  └────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────┘
```

## 7. Data Flow

```
Message In
    │
    ▼
┌────────────┐
│ Rule Match │─── no match ──▶ drop (logged)
└─────┬──────┘
      │ match
      ▼
┌────────────┐
│ VM Execute │
│            │
│  GATE      │── false ──▶ skip to PIPE
│  MAP       │
│  NEXT      │── success ──▶ update LastResponse
│  RETRY     │── fail ──▶ fallback or error
│  PARALLEL  │
│  COLLECT   │
│  EMIT      │── fire-and-forget
│  DROP      │── terminate
└─────┬──────┘
      │
      ▼
┌────────────┐
│ Complete   │
│ (events)   │
└────────────┘
```

## 8. Module Format (`.flow`)

```
┌───────────────────┐
│ Header (32 bytes) │  Magic, version, flags, counts
├───────────────────┤
│ Section Table     │  Type + offset + length entries
├───────────────────┤
│ Constant Pool     │  Deduplicated strings, URLs, operators
├───────────────────┤
│ Target Lists      │  Index arrays for PARALLEL/EMIT
├───────────────────┤
│ Instructions      │  16-byte fixed-size opcodes
├───────────────────┤
│ Map Expressions   │  Field paths and construct definitions
├───────────────────┤
│ Rule Metadata     │  Rule ID, version, priority
├───────────────────┤
│ Debug (optional)  │  Source mapping, comments
└───────────────────┘
```

## 9. Key Interfaces

```go
// Transport adapter
type Caller interface {
    Call(ctx, target, body) ([]byte, error)
}

// Fire-and-forget
type Emitter interface {
    Emit(ctx, target, body) error
}

// Circuit breaker
type Breaker interface {
    Allow(target) bool
    RecordSuccess(target)
    RecordFailure(target)
}

// Backpressure
type Credit interface {
    CanSend(target) bool
    Debit(target)
    Credit(target)
}

// Plugin
type Plugin interface {
    Transform(ctx, type, target, body) ([]byte, error)
}

// Event bus
type Bus interface {
    Subscribe(type, cap) <-chan Event
    Unsubscribe(type, <-chan Event)
    Publish(Event)
}
```

## 10. Module Loading and Verification

1. **Header check**: Magic bytes, version compatibility
2. **Section parse**: Validate offsets and lengths within bounds
3. **Constant pool**: Verify all index references are in range
4. **Instruction stream**: Opcode validity, jump targets in bounds
5. **Checksum**: SHA-256 of instruction stream (optional)
6. **Metadata**: Rule ID, version extracted for telemetry
