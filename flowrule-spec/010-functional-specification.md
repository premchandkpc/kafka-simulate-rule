# Functional Specification

## 1. Compilation Pipeline

```
Source text
  → Lexer (tokenization)
  → Parser (AST construction)
  → Semantic Analyzer (validation)
  → Optimizer (dead code elimination, constant folding, instruction merging)
  → Compiler (AST → execution plan)
  → Bytecode Encoder (execution plan → .flow binary)
  → Verifier (checksum, bounds, type consistency)
```

### 1.1 Lexer
- Input: UTF-8 text
- Output: Token stream
- Tokens are whitespace-separated
- Each token maps to one operation or modifier
- Errors on unrecognized token sequences

### 1.2 Parser
- Input: Token stream
- Output: Abstract syntax tree (instruction list)
- Validates structural rules:
  - COLLECT requires preceding PARALLEL
  - PIPE requires preceding GATE
  - RETRY/TIMEOUT must precede NEXT
  - No nested PARALLEL
  - DROP must be terminal

### 1.3 Optimizer
- Dead code elimination: instructions after DROP
- Consecutive EMIT merging: `e:a e:b` → `e:a,b`
- RETRY hoisting: `n:svc r3` → single instruction with RetryN=3
- TIMEOUT hoisting: `t500 n:svc` → single instruction with TimeoutMs=500

### 1.4 Compiler
- Resolves target names via registry (name → URL mapping)
- Validates gate operands (field, operator, value)
- Validates buffer counts (1–10000)
- Produces `ExecutionPlan` with rule ID, version, instruction list

### 1.5 Bytecode Encoder
- Serializes execution plan to binary `.flow` format
- Builds constant pool with deduplication
- Packs instructions into 16-byte fixed-size records
- Embeds section table for O(1) random access
- Appends rule metadata and checksum

## 2. Execution Pipeline

```
Bytecode module (.flow)
  → Module Loader (decode, verify)
  → VM Dispatcher (instruction loop)
  → Transport Layer (outbound calls)
  → Plugin Runtime (WASM transforms)
```

### 2.1 Module Loading
- Decode binary `.flow` format
- Verify checksum and version compatibility
- Build in-memory dispatch tables
- Pin constant pool for zero-copy access

### 2.2 Instruction Dispatch
- Sequential loop with jump/branch support
- Instruction pointer (IP) advancement
- No runtime parsing of DSL
- All data read from constant pool by index

### 2.3 Transport Calls
- NEXT: synchronous call with timeout, retry, backoff
- PARALLEL: concurrent fan-out with errgroup
- FALLBACK: conditional call on failure
- EMIT: fire-and-forget goroutine with 5s deadline

### 2.4 Plugin Execution
- Attachment points: GATE, MAP, PRE_CALL, POST_CALL
- WASM sandbox with 5s timeout per invocation
- Per-target and per-message plugin matching
- Module cache with precompilation

## 3. Instructions

| Opcode | Name | Args | Description |
|--------|------|------|-------------|
| 0 | NOP | — | No operation |
| 1 | NEXT | URL, [timeout], [retry] | Deliver to single target |
| 2 | PARALLEL | target list | Fan-out concurrently |
| 3 | COLLECT | — | Sync after parallel |
| 4 | FALLBACK | URL | Conditional on failure |
| 5 | GATE | field, op, value | Conditional branch |
| 6 | PIPE | — | End of gate-true branch |
| 7 | EMIT | target list | Fire-and-forget |
| 8 | DROP | — | Terminate processing |
| 9 | MAP | map index | Transform message |
| 10 | KEY | field | Extract partition key |
| 11 | SPLIT | field | Split by partition key |
| 12 | BUFFER | count | In-memory buffer |
| 13 | JUMP | target IP | Unconditional jump |
| 14 | JUMP_IF | target IP | Jump if failed |
| 15 | JUMP_IFN | target IP | Jump if not failed |

## 4. Gate Evaluation

Supported operators:
- `==` exact string match
- `!=` string inequality
- `>` `<` `>=` `<=` numeric comparison
- `contains` substring match

Field access supports dot-separated paths (`user.tier`).
Value type coercion: numbers compared as float64, booleans as strings, nested objects as JSON.

## 5. Reliability

### 5.1 Retry
- Configurable count (default 0)
- Exponential backoff: 100ms × 2^attempt, max 10s
- Jitter: random ±50% of backoff
- Context deadline applies across all attempts

### 5.2 Circuit Breaker
- States: CLOSED → OPEN → HALF-OPEN → CLOSED
- OPEN duration: configurable (default 30s)
- HALF-OPEN: single probe request
- Failure threshold: configurable count (default 5)
- Per-target state machine

### 5.3 Dead Letter Queue
- Durable storage (Badger-backed)
- 72h default TTL
- Stores: rule ID, target, error, retries, body, timestamp
- Snapshot API for inspection

### 5.4 Credit-Based Backpressure
- Per-target credit balance
- Initial credit: configurable (default 100)
- Debit on send attempt
- Credit on response (success or failure)
- Block when balance is zero

## 6. Hot Reload

- File watcher (fsnotify) monitors rule file
- New rules compiled and validated before activation
- Rule table swapped atomically (`atomic.Pointer`)
- In-flight messages complete with old rules
- New messages use new rules
- Failed compilation does not affect running rules

## 7. Lifecycle Events

| Event | Trigger | Data |
|-------|---------|------|
| msg.started | Message enters engine | msg_id, rule_id |
| msg.finished | Message completes | msg_id, hops, duration |
| msg.dropped | DROP instruction | msg_id, rule_id |
| msg.failed | Unrecoverable error | msg_id, error, stage |
| hop.succeeded | Successful NEXT | target, duration |
| hop.failed | Failed NEXT | target, error, retries |
| rule.matched | Rule matches message | rule_id, version |
| partition.assigned | KEY/SPLIT | partition_key |
