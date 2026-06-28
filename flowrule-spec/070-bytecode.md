# Bytecode Specification v1

## 1. Purpose

The FlowRule bytecode format (`.flow`) is a portable, immutable, verifiable binary representation of a compiled routing program. It is the contract between all implementations: any conforming runtime (Go, Rust, Java, etc.) can load and execute the same `.flow` file.

## 2. Format Overview

```
┌──────────────────────────────────────────────┐
│  Header (32 bytes)                            │
│  Magic: 0x464C4F57 ("FLOW")                   │
│  Version: major.minor                         │
│  Flags                                        │
│  Instruction count                            │
│  Constant count                               │
├──────────────────────────────────────────────┤
│  Section Table                                │
│  Section count (uint16)                       │
│  Reserved (6 bytes)                           │
│  Entry[0]: type, offset, length               │
│  Entry[1]: type, offset, length               │
│  ...                                          │
├──────────────────────────────────────────────┤
│  Constant Pool                                │
│  Entry[0]: type, length, payload              │
│  Entry[1]: type, length, payload              │
│  ...                                          │
├──────────────────────────────────────────────┤
│  Target Lists                                 │
│  List[0]: count, indices...                   │
│  List[1]: count, indices...                   │
│  ...                                          │
├──────────────────────────────────────────────┤
│  Instructions                                 │
│  Instr[0]: opcode, flags, arg1, arg2, arg3    │
│  Instr[1]: opcode, flags, arg1, arg2, arg3    │
│  ...                                          │
├──────────────────────────────────────────────┤
│  Map Expressions                              │
│  Expr[0]: type, body_length, body             │
│  Expr[1]: type, body_length, body             │
│  ...                                          │
├──────────────────────────────────────────────┤
│  Rule Metadata (optional)                     │
│  Version (int64)                              │
│  Priority (int32)                             │
│  RuleID length (uint32)                       │
│  RuleID (UTF-8 bytes)                         │
├──────────────────────────────────────────────┤
│  Debug Section (optional)                     │
└──────────────────────────────────────────────┘
```

## 3. Header (32 bytes)

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | Magic | `0x464C4F57` ("FLOW" in ASCII) |
| 4 | 1 | VersionMajor | 1 |
| 5 | 1 | VersionMinor | 0 |
| 6 | 2 | Flags | Bit field |
| 8 | 8 | Reserved | Zero |
| 16 | 8 | InstrCount | Number of instructions |
| 24 | 8 | ConstCount | Number of constants |

## 4. Section Table

After the header, the section table begins at offset 32.

| Offset | Size | Field |
|--------|------|-------|
| 32 | 2 | NumSections |
| 34 | 6 | Reserved |
| 40 | 16×N | Section entries |

Each section entry (16 bytes):

| Offset | Size | Field |
|--------|------|-------|
| 0 | 1 | SectionType |
| 1 | 7 | Reserved |
| 8 | 4 | Offset (from start of file) |
| 12 | 4 | Length |

Section types:
| Value | Name |
|-------|------|
| 1 | ConstPool |
| 2 | TargetLists |
| 3 | Instructions |
| 4 | MapExprs |
| 5 | RuleMeta |
| 6 | Debug |

## 5. Constant Pool

Each constant pool entry:

| Offset | Size | Field |
|--------|------|-------|
| 0 | 1 | ConstType |
| 1 | 3 | Reserved |
| 4 | 4 | PayloadLength (uint32 LE) |
| 8 | PayloadLength | Payload |

Const types:
| Value | Name | Payload |
|-------|------|---------|
| 1 | URL | UTF-8 URL string |
| 2 | STRING | UTF-8 string |
| 3 | FIELD_PATH | UTF-8 field path |
| 4 | OPERATOR | UTF-8 operator |
| 5 | VALUE | UTF-8 comparison value |
| 6 | KEY | UTF-8 partition key |
| 7 | MAP_EXPR | Opaque map expression body |

## 6. Target Lists

Each target list entry:

| Offset | Size | Field |
|--------|------|-------|
| 0 | 2 | Count (uint16 LE) |
| 2 | 2 | Reserved |
| 4 | 4×Count | Indices (uint32 LE array) |

## 7. Instructions (16 bytes each)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 1 | Opcode |
| 1 | 1 | Flags |
| 2 | 2 | Reserved |
| 4 | 4 | Arg1 (uint32 LE) |
| 8 | 4 | Arg2 (uint32 LE) |
| 12 | 4 | Arg3 (uint32 LE) |

### 7.1 Opcodes

| Value | Name | Arg1 | Arg2 | Arg3 |
|-------|------|------|------|------|
| 0 | NOP | — | — | — |
| 1 | NEXT | URL index / timeout | retry / URL index | URL index |
| 2 | PARALLEL | target list index | timeout | — |
| 3 | COLLECT | — | — | — |
| 4 | FALLBACK | URL index | — | — |
| 5 | GATE | field index | operator index | value index |
| 6 | PIPE | — | — | — |
| 7 | EMIT | target list index | — | — |
| 8 | DROP | — | — | — |
| 9 | MAP | map expr index | — | — |
| 10 | KEY | key index | — | — |
| 11 | SPLIT | key index | — | — |
| 12 | BUFFER | count | — | — |
| 13 | JUMP | target IP | — | — |
| 14 | JUMP_IF | target IP | — | — |
| 15 | JUMP_IFN | target IP | — | — |

### 7.2 Flags

| Bit | Name | Description |
|-----|------|-------------|
| 0 | HasTimeout | Arg1 contains timeout in ms |
| 1 | HasRetry | Arg2 contains retry count |

When both HasTimeout and HasRetry are set: Arg1=timeout, Arg2=retry, Arg3=URL index.
When only HasTimeout: Arg1=timeout, Arg2=URL index.
When only HasRetry: Arg1=URL index, Arg2=retry.
When neither: Arg1=URL index.

## 8. Map Expressions

Each map expression entry:

| Offset | Size | Field |
|--------|------|-------|
| 0 | 1 | MapExprType |
| 1 | 3 | Reserved |
| 4 | 4 | BodyLength (uint32 LE) |
| 8 | BodyLength | Body |

MapExpr types:
| Value | Name | Body format |
|-------|------|-------------|
| 1 | FIELD_PATH | segCount(2) + reserved(2) + segCPIndices(4×N) |
| 2 | ARRAY_INDEX | Array index (future) |
| 3 | ARRAY_FIELD | Array field (future) |
| 4 | CONSTRUCT | Object construction (future) |

## 9. Rule Metadata

| Offset | Size | Field |
|--------|------|-------|
| 0 | 8 | Version (int64 LE) |
| 8 | 4 | Priority (int32 LE) |
| 12 | 4 | RuleIDLength (uint32 LE) |
| 16 | RuleIDLength | RuleID (UTF-8) |

## 10. Checksum (optional)

If the VERIFIED flag is set in header flags, the last 32 bytes of the file contain SHA-256 of all preceding bytes. All implementations should verify this checksum before execution.

## 11. Portability

- All multi-byte integers are **little-endian**
- All strings are **UTF-8** without BOM
- No alignment padding beyond what is specified
- Implementations must ignore unknown section types
- Implementations must ignore unknown opcodes (treat as NOP with warning)
- Version compatibility: major version must match; minor version must be >= minimum supported
