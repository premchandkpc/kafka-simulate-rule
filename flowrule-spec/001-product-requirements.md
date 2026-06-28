# Product Requirements

## Overview

FlowRule enables teams to define message routing as declarative programs, compile them to verifiable bytecode, and execute them in a lightweight runtime embedded within their applications.

## Personas

### Platform Engineer
Owns the FlowRule deployment. Configures transports, plugins, monitoring, and capacity. Needs hot-reload, observability, and minimal operational overhead.

### Service Developer
Writes routing rules for their service. Does not want to learn a new platform. Needs a simple DSL, predictable behavior, and clear failure modes.

### SRE
Operates the system in production. Needs metrics, traces, logs, circuit breaker visibility, DLQ inspection, and the ability to safely roll back rule changes.

### Security Engineer
Reviews plugin and transport configurations. Needs sandbox guarantees, audit logs, and capability-based access control.

## Functional Requirements

### F1: DSL Compilation
- F1.1: Parse a declarative routing DSL into an AST
- F1.2: Validate semantic correctness (reachability, type safety)
- F1.3: Optimize the AST (dead code elimination, constant folding)
- F1.4: Compile to immutable bytecode
- F1.5: Embed metadata (rule ID, version, checksum) in bytecode
- F1.6: Produce human-readable compilation errors

### F2: Bytecode Execution
- F2.1: Load compiled `.flow` modules without runtime parsing
- F2.2: Execute instructions in a deterministic dispatch loop
- F2.3: Support 17+ opcodes: NOP, NEXT, PARALLEL, COLLECT, FALLBACK, GATE, PIPE, EMIT, DROP, MAP, KEY, SPLIT, BUFFER, JUMP, JUMP_IF, JUMP_IFN
- F2.4: Support constant pool, target lists, map expressions
- F2.5: Support instruction-level timeouts and retries

### F3: Message Routing
- F3.1: Route messages to one or more targets
- F3.2: Fan-out to parallel targets
- F3.3: Content-based conditional routing (gate)
- F3.4: Message transformation (MAP)
- F3.5: Emit fire-and-forget events
- F3.6: Split messages by partition key
- F3.7: Drop messages unconditionally

### F4: Reliability
- F4.1: Configurable retry with exponential backoff and jitter
- F4.2: Circuit breaker (closed→open→half-open)
- F4.3: Dead letter queue with configurable TTL
- F4.4: Fallback targets on failure
- F4.5: Credit-based backpressure per target

### F5: Transports
- F5.1: HTTP/1.1 and HTTP/2 client
- F5.2: gRPC client and server
- F5.3: Pluggable transport adapter interface
- F5.4: TLS for all transports

### F6: Plugins
- F6.1: WASM-based plugin sandbox
- F6.2: Plugin attachment points: GATE, MAP, PRE_CALL, POST_CALL
- F6.3: Per-target and global plugin scoping
- F6.4: Plugin timeout (context deadline)
- F6.5: Module caching and precompilation

### F7: Observability
- F7.1: Prometheus metrics (latency, throughput, errors, circuit breaker state)
- F7.2: OpenTelemetry traces per message hop
- F7.3: Structured logging (zerolog)
- F7.4: Lifecycle events (bus) for custom monitoring
- F7.5: DLQ inspection API

### F8: Management
- F8.1: CLI with subcommands: build, validate, inspect, run
- F8.2: Hot-reload rule table without restart
- F8.3: Graceful shutdown with in-flight drain
- F8.4: Signal handling (SIGTERM, SIGINT)

## Out of Scope (v1)

- Distributed execution across nodes
- Persistent rule storage (beyond files)
- Authentication/authorization
- Web dashboard
- Multi-language SDKs (single Go reference)
- Dynamic plugin loading at runtime
- Rule version rollback
- A/B rule testing
