# FlowRule — Kafka Message Routing VM

A **two-layer rule engine**: a Rust core (bytecode VM + DSL compiler) with a planned Go I/O shell.

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
├── go/            # Go I/O shell (skeleton)
├── config/        # Runtime configs
├── test/
│   ├── bench/         # Benchmarks
│   └── integration/   # Integration tests
└── docs/
    ├── specs/
    │   ├── dsl-syntax.md
    │   ├── bytecode-format.md
    │   ├── vm-architecture.md
    │   ├── ffi-api.md
    │   └── memory-management.md
    └── development.md
```

## Quick Start

```bash
cd rust && cargo test          # Run unit tests (82 tests)
cargo build --release          # Build shared lib
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Rust hot path, Go I/O | Performance-critical execution in Rust; Go for admin, observability, transport |
| 8-byte packed instructions | Cache-friendly, easy to snapshot/serialize |
| Slab pool for messages | Zero-alloc message lifecycle; `flowrule_msg_alloc` / `flowrule_msg_release` |
| DSL → bytecode compiler | Compile once, execute many; no parse cost per message |
| DAG as embedded sub-language | Complex routing expressed declaratively; validated at compile time |
