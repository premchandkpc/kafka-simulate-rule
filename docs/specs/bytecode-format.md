# Bytecode Format Specification

## Overview

The `ExecutionPlan` is the compiled output of the DSL compiler. It is a compact, serializable binary format designed for:
- Cache-friendly linear execution
- Snapshot/serialize to disk or wire
- Zero-deserialization execution from raw bytes

## Instruction Encoding

Each instruction is **8 bytes** packed into a `u64`:

```
Bit:  63..48  47..32  31..16  15..8  7..0
      +-------+-------+-------+------+------+
      |   c   |   b   |   a   |flag  |opcode|
      +-------+-------+-------+------+------+
       u16     u16     u16     u8     u8
```

### Fields

| Field | Bits | Type | Description |
|-------|------|------|-------------|
| `opcode` | 7:0 | u8 | Operation code (0–22 reserved) |
| `flags` | 15:8 | u8 | Per-opcode modifier flags |
| `a` | 31:16 | u16 | Primary operand (const ID, service ID, chunk size, etc.) |
| `b` | 47:32 | u16 | Secondary operand |
| `c` | 63:48 | u16 | Tertiary operand |

### Rust Representation

```rust
#[repr(C)]
pub struct Instruction {
    data: u64,
}
```

Builder methods provide ergonomic construction:

```rust
Instruction::next(service_id)
    .with_timeout(timeout_ms)
    .with_mode(Async | Buffer)
```

## Opcode Table

| # | Opcode | a | b | c | Description |
|---|--------|---|---|---|-------------|
| 0 | `Nop` | — | — | — | No-op (optimized away) |
| 1 | `Next` | service_id | timeout_ms | mode_flags | Forward message to service |
| 2 | `Async` | service_id | timeout_ms | — | Fire-and-forget call |
| 3 | `Parallel` | count | — | — | Fan-out; operands in service table |
| 4 | `Collect` | — | — | — | Merge parallel results |
| 5 | `Fallback` | service_id | — | — | Route on failure |
| 6 | `Gate` | then_offset | else_offset | op_code | Conditional branch |
| 7 | `Split` | — | — | — | Split array into records |
| 8 | `Map` | expr_id | — | — | Transform fields via MapExpr |
| 9 | `Emit` | count | — | — | Fire-and-forget to N services |
| 10 | `Drop` | — | — | — | Halt execution |
| 11 | `Buffer` | service_id | timeout_ms | — | Non-blocking buffered send |
| 12 | `Key` | expr_id | — | — | Set routing key |
| 13 | `Retry` | count | strategy | interval_ms | Retry policy for preceding op |
| 14 | `Pipe` | — | — | — | No-op separator |
| 15 | `Timeout` | timeout_ms | — | — | Set timeout for next call |
| 16 | `Chunk` | chunk_size | mode | — | Split message into chunks |
| 17 | `Dag` | node_count | terminal_count | layer_count | DAG execution root |
| 18 | `Jmp` | offset | — | — | Unconditional jump |
| 19 | `Label` | label_id | — | — | Jump target marker |
| 20 | `SvcArg` | service_id | arg_id | — | Service argument |
| 21 | `RetryData` | count | strategy | interval_ms | Inline retry config |
| 22 | `JumpOffset` | offset | — | — | Resolved jump offset |

## ExecutionPlan

```rust
pub struct ExecutionPlan {
    pub instructions: Vec<Instruction>,    // Linear bytecode
    pub constant_pool: ConstantPool,       // Interned strings
    pub service_table: ServiceTable,       // Service name → ID mapping
    pub dag_table: Option<DAGTable>,       // DAG node/layer info
    pub chunk_configs: Vec<ChunkConfig>,   // Chunk operation metadata
    pub retry_configs: Vec<RetryConfig>,   // Retry policy metadata
    pub map_exprs: Vec<MapExpr>,           // Map expression descriptors
}
```

### ConstantPool

String interning table. Constants are stored deduplicated and referenced by u16 ID.

```rust
pub struct ConstantPool {
    pub strings: Vec<String>,
    pub lookup: HashMap<String, u16>,
}
```

### ServiceTable

Maps service names to u16 IDs for compact instruction operands.

```rust
pub struct ServiceTable {
    pub services: Vec<String>,
    pub lookup: HashMap<String, u16>,
}
```

### DAGTable

```rust
pub struct DAGTable {
    pub nodes: Vec<DAGNode>,         // All DAG nodes
    pub layers: Vec<Vec<u16>>,       // Layer-by-layer execution order
    pub terminal_ids: Vec<u16>,      // Terminal nodes (no dependents)
}
```

### MapExpr

```rust
pub struct MapExpr {
    pub dest_field: String,          // Target field path
    pub source_field: Option<String>,// Source field (copy mode), None for expr mode
    pub expression: Option<String>,  // Raw expression string (expr mode)
}
```

### RetryConfig

```rust
pub struct RetryConfig {
    pub max_retries: u8,
    pub strategy: RetryStrategy,     // Exp | Linear | Fixed
    pub interval_ms: u16,
}
```

### ChunkConfig

```rust
pub struct ChunkConfig {
    pub chunk_size: u16,
    pub mode: ChunkMode,             // Seq | Par
}
```

## Serialization

The `ExecutionPlan` supports `bincode` serialization for:

```rust
let bytes = bincode::serialize(&plan).unwrap();
let plan: ExecutionPlan = bincode::deserialize(&bytes).unwrap();
```

This enables:
- Pre-compiling plans offline and loading at startup
- Sharing compiled plans across nodes
- Snapshotting hot-reloadable rule sets
