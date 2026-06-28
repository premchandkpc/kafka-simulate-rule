# Development Guide

## Prerequisites

- Rust 1.70+ (edition 2021)
- Go 1.21+ (when working on Go layer)
- No other system dependencies

## Build

```bash
cd rust

# Debug build
cargo build

# Release build (LTO, optimized)
cargo build --release

# Shared library only
cargo build --release --lib
```

## Test

```bash
cd rust

# Run all tests (82 unit tests)
cargo test

# Run specific test module
cargo test executor::expr
cargo test dsl::lexer
cargo test dsl::parser
cargo test dsl::compiler
cargo test dsl::optimizer

# Run specific test
cargo test test_vm_map

# Run with output
cargo test -- --nocapture
```

## Project Layout

```
rust/src/
├── lib.rs              # C FFI exports, module declarations
├── bytecode/           # Instruction set and plan types
│   ├── mod.rs
│   ├── opcode.rs       # Opcode enum + metadata
│   ├── instruction.rs  # 8-byte packed Instruction
│   ├── consts.rs       # ConstantPool
│   ├── services.rs     # ServiceTable
│   ├── dag_table.rs    # DAGTable
│   ├── mapexpr.rs      # MapExpr
│   └── plan.rs         # ExecutionPlan
├── dsl/                # Language toolchain
│   ├── mod.rs
│   ├── lexer.rs        # Tokenizer
│   ├── parser.rs       # AST builder
│   ├── optimizer.rs    # AST optimizations
│   └── compiler.rs     # AST → ExecutionPlan
├── executor/           # Virtual machine
│   ├── mod.rs          # VM dispatch loop
│   ├── next.rs         # Service call + retry
│   ├── parallel.rs     # Parallel fan-out
│   ├── gate.rs         # Conditional branch
│   ├── emit.rs         # Fire-and-forget
│   ├── map.rs          # Field transformation
│   ├── dag.rs          # DAG execution
│   ├── chunk.rs        # Chunk processing
│   ├── helpers.rs      # JSON utilities
│   └── expr.rs         # Expression engine
└── memory/             # Memory management
    ├── mod.rs
    ├── arena.rs        # Bump allocator
    ├── slab.rs         # Slab pool
    └── intern.rs       # String interning
```

## Adding a New Opcode

1. Define opcode in `bytecode/opcode.rs` — add variant to `Opcode` enum
2. Add builder in `bytecode/instruction.rs` — `Instruction::your_op()`
3. Handle in `dsl/lexer.rs` — add token variant and lex logic
4. Handle in `dsl/parser.rs` — add AST node and parse logic
5. Handle in `dsl/optimizer.rs` — add optimization rules if needed
6. Emit in `dsl/compiler.rs` — compile AST to instruction
7. Execute in `executor/mod.rs` — add arm in `dispatch()`
8. Add op handler with test coverage

## Adding a Built-in Function (Expression Engine)

1. Define in `executor/expr.rs`:
   - Add function name to `eval_expr()` match
   - Add logic in `exec_builtin()` match
2. Add test covering the new function

## Conventions

- **Naming:** snake_case for functions/vars, CamelCase for types
- **Errors:** Use `thiserror` derive macros; return `Result<_, ExecError>` or `Result<_, CompileError>`
- **Testing:** Unit tests inline in source files (`#[cfg(test)]`); integration tests in `test/integration/`
- **FFI safety:** All `extern "C"` functions check null pointers; return error codes

## Go Layer (Skeleton)

The Go shell is planned but not yet implemented. Structure:

```
go/
├── cmd/flowrule/           # Entry point
└── internal/
    ├── bridge/             # cgo bindings to Rust
    ├── engine/             # Rule lifecycle management
    ├── flow/               # Flow orchestration
    ├── transport/          # Kafka I/O (reader/writer)
    ├── admin/              # Admin API
    ├── observability/      # Metrics, tracing
    └── reliability/        # Circuit breakers, retry
```

## Performance Considerations

- VM dispatch uses `match` on opcode — compiler generates jump table
- Service calls are FFI-bound (C callbacks into Go); overhead dominated by serialization, not dispatch
- Slab pool should be sized to workload peak concurrency
- Expression engine uses simple recursive descent — no parser generator dependency
