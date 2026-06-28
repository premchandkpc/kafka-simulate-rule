# FlowRule — Kafka Message Routing VM

A **two-layer rule engine**: Rust core (bytecode VM + DSL compiler) + Go I/O shell.

## Project Map

```
flowrule
├── rust/          # Core engine (DSL → bytecode → VM)
│   ├── src/
│   │   ├── bytecode/   # Instruction set, plan format, const pool
│   │   ├── dsl/        # Lexer, parser, optimizer, compiler
│   │   ├── executor/   # VM dispatch + op handlers
│   │   └── memory/     # Arena, slab pool, string interning
│   └── Cargo.toml
├── go/            # Go I/O shell
│   ├── cmd/flowrule/   # Entry point
│   └── internal/
│       ├── bridge/         # cgo bindings to Rust FFI
│       ├── engine/         # Rule lifecycle management
│       ├── flow/           # Flow orchestration
│       ├── transport/      # Kafka I/O (consumer/producer)
│       ├── admin/          # Admin HTTP API
│       ├── observability/  # Metrics
│       └── reliability/    # Circuit breaker
├── docs/
│   ├── specs/
│   │   ├── dsl-syntax.md
│   │   ├── bytecode-format.md
│   │   ├── vm-architecture.md
│   │   ├── ffi-api.md
│   │   └── memory-management.md
│   └── development.md
```

## Quick Start

```bash
# Rust — compile shared lib + run tests
cd rust && cargo build --release && cargo test

# Go — build binary + run tests
cd .. && make test

# Full build
make
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Rust hot path, Go I/O | Performance-critical execution in Rust; Go for admin, observability, transport |
| 8-byte packed instructions | Cache-friendly, easy to snapshot/serialize |
| Slab pool for messages | Zero-alloc message lifecycle; `flowrule_msg_alloc` / `flowrule_msg_release` |
| DSL → bytecode compiler | Compile once, execute many; no parse cost per message |
| DAG as embedded sub-language | Complex routing expressed declaratively; validated at compile time |
| Go service caller bridge | Rust VM calls back into Go via `//export` + C helper; enables service dispatch in Go |
