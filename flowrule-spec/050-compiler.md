# Compiler Specification

## 1. Purpose

The Compiler transforms FlowRule DSL source text into verifiable, portable bytecode. It is the only path from human-readable configuration to machine-executable programs.

## 2. Pipeline

```
Source text (string)
    │
    ▼
┌──────────────┐
│    Lexer      │  Tokenization (whitespace-separated tokens)
└──────┬───────┘
       │ tokens
       ▼
┌──────────────┐
│   Parser      │  AST construction + structural validation
└──────┬───────┘
       │ instructions
       ▼
┌──────────────┐
│  Optimizer    │  Dead code elimination, instruction merging, hoisting
└──────┬───────┘
       │ optimized instructions
       ▼
┌──────────────┐
│  Compiler     │  Target resolution, semantic validation
└──────┬───────┘
       │ execution plan
       ▼
┌──────────────┐
│  Encoder      │  Bytecode serialization (binary .flow)
└──────┬───────┘
       │ .flow bytes
       ▼
┌──────────────┐
│  Verifier     │  Checksum, bounds, cross-reference validation
└──────────────┘
```

## 3. Lexer

### 3.1 Input/Output
- Input: UTF-8 string
- Output: `[]Token` or error
- Tokens are delimited by whitespace (spaces, tabs, newlines)

### 3.2 Token Grammar

```
token    = timeout | retry | buffer | next | parallel | collect
         | fallback | gate | pipe | split | map | emit | drop | key

timeout  = "t" digit+
retry    = "r" digit+
buffer   = "b" digit+
next     = "n:" target-name
parallel = "p:" target-name ("," target-name)+
collect  = "c"
fallback = "f:" target-name
gate     = "g:" field-path operator value
pipe     = "|"
split    = "s:" field
map      = "m:" map-expr
emit     = "e:" target-name ("," target-name)*
drop     = "d"
key      = "k:" field

operator = "==" | "!=" | ">=" | "<=" | ">" | "<" | "contains"
digit    = "0" | "1" | ... | "9"
```

### 3.3 Lexing Rules
- Each token must be complete (no partial tokens)
- Invalid tokens produce a `LexError` with the unrecognized text
- Empty input produces zero tokens
- Tokens are case-sensitive

## 4. Parser

### 4.1 Input/Output
- Input: `[]Token`
- Output: `[]Instruction` or error

### 4.2 Structural Rules
- `c` (collect) must be preceded by `p:` (parallel)
- `|` (pipe) must be preceded by `g:` (gate)
- `r` (retry) and `t` (timeout) modify the preceding instruction
- Multiple consecutive `e:` are merged
- `d` (drop) must be terminal
- Only one outstanding parallel block at a time (no nesting)

### 4.3 Gate Parsing
Gate tokens have format `g:<field><op><value>`. The parser splits the string after `g:` into field, operator, and value:
- Operator is the first match of: `==`, `!=`, `>=`, `<=`, `>`, `<`, `contains`
- Field is everything before the operator
- Value is everything after the operator
- Field must be non-empty
- Operator must be non-empty and match one of the supported set

## 5. Optimizer

### 5.1 Transformations
1. **Dead code elimination**: Remove instructions after `d` (drop)
2. **Emit merging**: `e:a e:b` → single `e:` with targets [a, b]
3. **Retry hoisting**: `n:svc r3` → merge retry count into next instruction
4. **Timeout hoisting**: `t500 n:svc` → merge timeout into next instruction
5. **Orphan removal**: Remove standalone `r`, `t` without preceding instruction

### 5.2 Invariants
- Optimizer must never change program semantics
- Optimizer output must be valid parser output
- Optimization is idempotent (running twice produces same result)

## 6. Compiler

### 6.1 Input/Output
- Input: `[]Instruction`, `TargetRegistry`, `RuleID`, `Version`
- Output: `*ExecutionPlan` or error

### 6.2 Target Resolution
Target names in instructions (`n:`, `p:`, `f:`, `e:`) are resolved via the registry:
- Lookup name in `map[string]string` registry
- Replace name with full URL
- Error on unresolvable target
- Error on empty target

### 6.3 Validation
- Gate: field must be non-empty
- Key/Split: field must be non-empty
- Buffer: count must be 1–10000
- Map: expression must be non-nil
- Instructions: must not exceed 2^32-1

## 7. Bytecode Encoder

See [Bytecode Specification](070-bytecode.md) for the binary format.

## 8. Verifier

Post-encoding verification:
- Instruction count matches header
- All constant pool indices are in range
- All jump targets are in bounds
- All target list indices are in range
- Map expression indices are in range
- Optional SHA-256 checksum
