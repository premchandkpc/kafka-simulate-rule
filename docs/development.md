# Development Guide

## Prerequisites

- Rust 1.70+ (edition 2021)
- Go 1.22+
- No other system dependencies

## Build

```bash
# Full build (Rust cdylib + Go binary)
make

# Rust only
cd rust
cargo build --release

# Go only (requires prebuilt Rust cdylib)
make go
```

The Rust library is built as both `cdylib` and `rlib`. The `cdylib` (`libflowrule_core.dylib`/`.so`) is linked by the Go shell via cgo.

## Test

```bash
# All tests (Rust 82 + Go 33)
make test

# Rust only
cd rust && cargo test

# Go only
CGO_ENABLED=1 go test ./go/...

# Go lint
CGO_ENABLED=1 go vet ./go/...
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
├── ffi.rs              # extern "C" exports for Go bridge
└── memory/             # Memory management
    ├── mod.rs
    ├── arena.rs        # Bump allocator
    ├── slab.rs         # Slab pool
    └── intern.rs       # String interning

go/
├── cmd/flowrule/           # Entry point (HTTP admin + consumer)
└── internal/
    ├── bridge/             # cgo bindings to Rust FFI
    │   ├── bridge.go       # Go wrappers + //export callback
    │   ├── caller_bridge.c # C helper for function pointer callback
    │   └── bridge_test.go  # 11 integration tests
    ├── engine/             # Rule lifecycle (Deploy, Remove, ExecuteAll)
    ├── flow/               # Flow orchestrator with state machine
    ├── transport/          # Kafka consumer/producer
    ├── admin/              # HTTP admin API (POST/DELETE/GET rules)
    ├── observability/      # Metrics counters
    └── reliability/        # Circuit breaker
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
- **Testing:** Rust unit tests inline in source files (`#[cfg(test)]`); Go test files alongside source
- **FFI safety:** All `extern "C"` functions check null pointers; return error codes
- **Go cgo pattern:** Callbacks use `//export` + C helper file (`caller_bridge.c`) to pass Go functions as C function pointers

## Performance Considerations

- VM dispatch uses `match` on opcode — compiler generates jump table
- Service calls are FFI-bound (C callbacks into Go); overhead dominated by serialization, not dispatch
- Slab pool should be sized to workload peak concurrency
- Expression engine uses simple recursive descent — no parser generator dependency
