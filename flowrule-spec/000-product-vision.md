# Product Vision

## Elevator Pitch

FlowRule is an embedded execution platform that compiles declarative routing programs into immutable bytecode executed by a lightweight virtual machine. It provides predictable, observable, low-latency message execution across heterogeneous transports without coupling routing logic to application code.

## What FlowRule Is

FlowRule is simultaneously:

- A **compiler** — domain-specific language → verifiable bytecode
- A **virtual machine** — deterministic instruction dispatch
- A **runtime** — lifecycle, scheduling, resource management
- An **execution engine** — message routing, transformation, delivery
- A **routing platform** — content-based routing, split, merge, parallel
- A **policy engine** — circuit breakers, retries, backpressure
- A **transport layer** — HTTP, gRPC, Kafka, pluggable adapters
- A **plugin runtime** — WASM sandbox for custom logic

No mention of Go. No mention of Rust. No HTTP. Technology never defines the product.

## What FlowRule Is Not

FlowRule will not:

- Store business data
- Replace Kafka or message queues
- Replace databases
- Replace Kubernetes or service discovery
- Replace API gateways
- Replace workflow orchestrators (Temporal, Airflow)
- Replace authentication providers
- Manage infrastructure

Scope control is what keeps great projects great.

## Product Philosophy

```
Compile once.        Execute forever.
Configuration becomes programs.
Programs become bytecode.
Bytecode becomes immutable.
Execution is deterministic.
Everything observable.
Everything replaceable.
Everything measurable.
Everything testable.
Everything versioned.
Everything reproducible.
```

## Why This Exists

Distributed systems share a common pattern: receive a message, evaluate conditions, route to one or more services, handle failures, emit events. Teams implement this pattern repeatedly — in different languages, with different libraries, inconsistent reliability, and no unified observability.

FlowRule extracts this pattern into a reusable platform:

- **Before:** Every service couples routing logic with business logic. Retries, timeouts, circuit breakers, and conditional routing are reimplemented per team.
- **After:** Routing is a compiled program. The runtime handles reliability, observability, and transport. Business logic stays pure.

## Design Tenets

1. **Specification-first.** The spec is the primary artifact. Implementations are secondary. The product must outlive any individual programming language.
2. **Language agnostic.** FlowRule is not a Go project or a Rust project. It is a specification with reference implementations.
3. **Immutable execution.** Once compiled, a program never changes. Hot reload creates new programs; old programs drain gracefully.
4. **Deterministic by default.** Given the same input and the same program, every execution produces the same output.
5. **Zero-copy where possible.** Messages flow through the runtime without unnecessary allocation.
6. **Observability is a feature, not a bolt-on.** Every hop, every decision, every failure is an event.
7. **Security is not optional.** Plugins are sandboxed. Transports are encrypted. Programs are verified.
8. **Replaceable subsystems.** The compiler, runtime, VM, scheduler, and transports are each defined by stable contracts. Swap any component without affecting others.

## Success Criteria

| Dimension | Measure |
|-----------|---------|
| Correctness | All programs produce identical results across implementations |
| Performance | Cold start <10ms, execution overhead <50μs, P99 <1ms |
| Maintainability | Each subsystem fits in one spec document |
| Extensibility | New transport in <500 lines, new plugin in WASM |
| Observability | Every event emitted, every metric exposed |
| Reliability | No data loss, predictable failure modes |
| Security | Plugin sandbox escapes = 0 |
| Developer Experience | `flowrule build` single command to production |
| Portability | Same `.flow` file runs on Rust VM and Go VM |
