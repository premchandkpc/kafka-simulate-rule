# FlowRule Bytecode Specification — v1

> A portable, deterministic, binary bytecode format for message routing programs.
> Compiled once. Executed everywhere. Zero parsing at runtime.

---

## 1. Design Principles

1. **Numeric only** — no strings, no pointers, no heap references in instruction stream.
2. **Fixed-size instructions** — 16 bytes each. Cache-line friendly. Predictable dispatch.
3. **Constant pool** — all variable-length data (URLs, field paths, strings) stored in a typed pool. Instructions reference pool indices.
4. **Deterministic** — same source → same bytecode. No ASLR. No pointer embedding.
5. **Versioned** — magic + version at offset 0 enables forward/backward compat.
6. **Self-contained** — bytecode carries all metadata needed for execution. No external config.

---

## 2. Module Structure

A FlowRule module is a single binary blob with the following layout:

```
┌─────────────────────────────────────┐
│           Module Header             │   32 bytes
├─────────────────────────────────────┤
│           Section Table             │   8 + N*16 bytes
├─────────────────────────────────────┤
│         Constant Pool Section       │
├─────────────────────────────────────┤
│         Target List Section         │
├─────────────────────────────────────┤
│        Instruction Section          │
├─────────────────────────────────────┤
│       Map Expression Section        │
├─────────────────────────────────────┤
│         Debug Info Section          │   optional
└─────────────────────────────────────┘
```

### 2.1 Module Header (32 bytes)

```
Offset  Size  Field            Description
──────  ────  ─────            ───────────
0       4     magic            "FLOW" (0x464C4F57)
4       1     version_major    1
5       1     version_minor    0
6       2     flags            bitfield (reserved, must be 0)
8       1     opcode_version   opcode set version (must match runtime)
9       7     reserved         zero-filled
16      8     num_instructions  number of instructions in instruction section
24      8     num_constants    number of entries in constant pool
```

Total: 32 bytes.

### 2.2 Section Table

```
Offset  Size  Field            Description
──────  ────  ─────            ───────────
0       2     section_count    number of sections (N)
2       6     reserved
8       N*16  sections[]       array of SectionEntry
```

Each SectionEntry (16 bytes):

```
Offset  Size  Field            Description
──────  ────  ─────            ───────────
0       1     type             section type (see below)
1       7     reserved
8       4     offset           byte offset from start of module
12      4     length           byte length of section
```

Section types:

```
Type  Name               Description
────  ────               ───────────
0x01  CONSTANT_POOL      typed constant entries
0x02  TARGET_LISTS       arrays of constant pool indices for multi-target instrs
0x03  INSTRUCTIONS       compiled instruction stream
0x04  MAP_EXPRESSIONS    parsed map expression structures
0x05  DEBUG              optional debug info (line numbers, source mapping)
0x06  RULE_META          rule metadata (ID, version, priority, match config)
```

---

## 3. Constant Pool

The constant pool stores all variable-length data referenced by instructions.
Each entry has a 4-byte header followed by type-specific data.

### 3.1 Entry Header (4 bytes)

```
Offset  Size  Field            Description
──────  ────  ─────            ───────────
0       1     type             constant type (see below)
1       3     length           byte length of payload (excluding header)
```

### 3.2 Constant Types

```
Type  Name          Payload
────  ────          ───────
0x01  STRING        raw UTF-8 bytes (null-terminated, null not counted in length)
0x02  URL           UTF-8 URL string (null-terminated)
0x03  FIELD_PATH    dot-separated field path, e.g. "user.tier" (null-terminated)
0x04  OPERATOR      comparison operator: "==", "!=", ">", "<", ">=", "<=", "contains"
0x05  VALUE         comparison value string (null-terminated)
0x06  TARGET_NAME   target name for resolution (null-terminated)
0x07  KEY           partition key / split field name (null-terminated)
0x08  MAP_EXPR      serialized map expression (see Map Expression Section)
```

### 3.3 Pool Layout

Constant pool entries are stored sequentially in the CONSTANT_POOL section.
Indexing is 0-based. An instruction referencing constant pool index 5 means
the 6th entry in the pool.

```
┌──────────────────┐
│ Entry 0 header   │   4 bytes
│ Entry 0 payload  │   length bytes
├──────────────────┤
│ Entry 1 header   │   4 bytes
│ Entry 1 payload  │   length bytes
├──────────────────┤
│ ...              │
└──────────────────┘
```

---

## 4. Instruction Encoding

Each instruction is exactly **16 bytes** with the following fixed layout:

```
Offset  Size  Field    Description
──────  ────  ─────    ───────────
0       1     opcode   instruction opcode
1       1     flags    bitfield flags
2       2     pad      reserved (zero)
4       4     arg1     operand 1 (interpretation depends on opcode)
8       4     arg2     operand 2 (interpretation depends on opcode)
12      4     arg3     operand 3 (interpretation depends on opcode)
```

Total: 16 bytes. No alignment padding needed (already 16-byte aligned).

### 4.1 Flags

```
Bit   Name        Description
───   ────        ───────────
0     HAS_TIMEOUT  timeout_ms is set (arg1 = timeout in milliseconds)
1     HAS_RETRY    retry_n is set (arg2 = max retry count)
2-7   RESERVED     must be 0
```

When flags.0 (HAS_TIMEOUT) = 1: arg1 contains the timeout value in milliseconds.
When flags.1 (HAS_RETRY) = 1: arg2 contains the retry count.

For opcodes where arg1/arg2 have specific meanings (see below), timeout and retry
use flags + dedicated operand slots to avoid conflict.

### 4.2 Opcode Table

```
Opcode  Name       Args                     Description
──────  ────       ────                     ───────────
0x01    NOP        —                        no operation
0x02    NEXT       arg1=url_cp_idx          send to target URL, await response
0x03    PARALLEL   arg1=target_list_idx     fan-out to targets concurrently
0x04    COLLECT    —                        await all parallel branches, merge
0x05    FALLBACK   arg1=url_cp_idx          route here if msg.failed == true
0x06    GATE       arg1=field_cp_idx        conditional: field_path op value
                   arg2=op_cp_idx           if false → skip to pipe/end
                   arg3=value_cp_idx
0x07    PIPE       —                        alternative branch separator
0x08    EMIT       arg1=target_list_idx     fire-and-forget to targets
0x09    DROP       —                        discard message silently
0x0A    MAP        arg1=map_expr_idx        transform payload via map expression
0x0B    KEY        arg1=field_cp_idx        set partition key from field
0x0C    SPLIT      arg1=field_cp_idx        extract partition key for ordering
0x0D    BUFFER     arg1=count               batch N messages before release
0x0E    SEND       arg1=url_cp_idx          alias for NEXT (semantic clarity)
0x0F    JUMP       arg1=instruction_idx     unconditional jump
0x10    JUMP_IF    arg1=instruction_idx     jump if msg.failed == true
0x11    JUMP_IFN   arg1=instruction_idx     jump if msg.failed == false
```

Opcodes 0x01–0x0F map directly to the current DSL instruction set.
Opcodes 0x0E–0x11 extend the set for control flow.

### 4.3 Operand Semantics by Opcode

| Opcode     | arg1                | arg2              | arg3                |
|------------|---------------------|-------------------|---------------------|
| NEXT       | url CP index        | —                 | —                   |
| PARALLEL   | target list index   | —                 | —                   |
| COLLECT    | —                   | —                 | —                   |
| FALLBACK   | url CP index        | —                 | —                   |
| GATE       | field path CP idx   | operator CP idx   | value CP idx        |
| PIPE       | —                   | —                 | —                   |
| EMIT       | target list index   | —                 | —                   |
| DROP       | —                   | —                 | —                   |
| MAP        | map expr CP idx     | —                 | —                   |
| KEY        | field CP idx        | —                 | —                   |
| SPLIT      | field CP idx        | —                 | —                   |
| BUFFER     | count (literal)     | —                 | —                   |
| JUMP       | instruction index   | —                 | —                   |
| JUMP_IF    | instruction index   | —                 | —                   |
| JUMP_IFN   | instruction index   | —                 | —                   |

When flags HAS_TIMEOUT is set, arg1 shifts:
- For NEXT, FALLBACK, SEND: arg1 = timeout_ms, arg2 = url_cp_idx
- For PARALLEL: arg1 = timeout_ms, arg2 = target_list_idx

When flags HAS_RETRY is set, arg2 shifts:
- For NEXT, FALLBACK, SEND: arg2 = retry_n, arg3 = url_cp_idx (or arg1 = timeout_ms, arg2 = retry_n, arg3 = url_cp_idx when both set)

This keeps the encoding compact while supporting optional modifiers.

---

## 5. Target List Section

The TARGET_LISTS section stores arrays of constant pool indices for
instructions that have multiple targets (PARALLEL, EMIT).

### 5.1 Target List Entry

```
Offset  Size  Field              Description
──────  ────  ─────              ───────────
0       2     count              number of targets
2       2     reserved
4       4*N   target_cp_indices  array of constant pool indices (uint32 each)
```

Each entry is variable-length: 4 + 4*count bytes.
Entries are indexed sequentially starting at 0.

---

## 6. Map Expression Section

The MAP_EXPRESSIONS section stores parsed map expression structures.
Each expression is a self-contained binary structure.

### 6.1 Map Expression Entry Header (4 bytes)

```
Offset  Size  Field              Description
──────  ────  ─────              ───────────
0       1     expr_type          0=field_path, 1=array_index, 2=array_field, 3=construct
1       3     body_length         byte length of expression body
```

### 6.2 Expression Body Types

**Field Path (expr_type=0):**

```
Offset  Size  Field              Description
──────  ────  ─────              ───────────
0       2     segment_count       number of path segments
2       2     reserved
4       4*N   segment_cp_indices  CP indices for each path segment (STRING type)
```

**Array Index (expr_type=1):**

```
Offset  Size  Field              Description
──────  ────  ─────              ───────────
0       2     field_cp_idx       CP index for array field name
2       2     index              array index (uint16)
```

**Construct (expr_type=3):**

```
Offset  Size  Field              Description
──────  ────  ─────              ───────────
0       2     kv_count            number of key-value pairs
2       6     reserved
8       16*N  kv_pairs[]          array of MapKV (16 bytes each)
```

Each MapKV (16 bytes):

```
Offset  Size  Field              Description
──────  ────  ─────              ───────────
0       4     key_cp_idx         CP index for output key name (STRING type)
4       2     path_seg_count      number of path segments in source field
6       2     reserved
8       4*N   seg_cp_indices      CP indices for each path segment
```

---

## 7. Execution Model

### 7.1 Virtual Machine State

```
Instruction Pointer (IP)     → index into instruction array (uint32)
Constant Pool (CP)           → array of typed byte slices
Target Lists                 → array of uint32 lists
Map Expressions              → array of parsed map expressions
Message                      → current message context (external, not in bytecode)
Failed Flag                  → boolean, set by execution, read by FALLBACK/JUMP_IF
Stack                        → operand stack for future extensibility
```

### 7.2 Execution Loop (Pseudocode)

```
func execute(plan, msg):
    ip = 0
    while ip < len(plan.instructions):
        instr = plan.instructions[ip]
        switch instr.opcode:
            NEXT:       send_to_url(plan.cp[instr.arg1], msg)
                        if failed and not fallback_ahead: return error
            PARALLEL:   spawn_branches(plan.target_lists[instr.arg1], msg)
            COLLECT:    await_all(), merge(msg)
            FALLBACK:   if msg.failed: send_to_url(plan.cp[instr.arg1], msg)
            GATE:       if not eval_gate(msg, plan.cp, instr):
                            ip = skip_to_pipe(plan, ip)
            PIPE:       ip = skip_to_end(plan, ip)
            EMIT:       fire_and_forget(plan.target_lists[instr.arg1], msg)
            DROP:       return nil
            MAP:        apply_map(plan.map_exprs[instr.arg1], msg)
            KEY/SPLIT:  set_partition_key(msg, plan.cp[instr.arg1])
            BUFFER:     buffer_message(msg, instr.arg1)
            JUMP:       ip = instr.arg1; continue
            JUMP_IF:    if msg.failed: ip = instr.arg1; continue
            JUMP_IFN:   if not msg.failed: ip = instr.arg1; continue
        ip++
```

### 7.3 Register Conventions (Future)

```
R0–R3:   General purpose message references
R4:      Current instruction pointer
R5:      Failed flag (read-only)
R6:      Hop count
R7:      Scratch
```

Registers are not part of v1. Reserved for future optimization.

---

## 8. Serialization

### 8.1 Byte Order

All multi-byte integers are **little-endian** (matching x86, ARM, WASM).

### 8.2 Alignment

All sections start at offset aligned to 8 bytes.
Pad bytes between sections must be zero.

### 8.3 Magic Bytes

Magic: `0x46 0x4C 0x4F 0x57` = "FLOW"

---

## 9. Example Bytecode

### 9.1 DSL: `t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill`

#### Constant Pool

```
Idx  Type     Value
───  ────     ─────
0    URL      "http://validate:8080/validate"
1    URL      "http://fraud-svc:8080/check"
2    URL      "http://inventory-svc:8080/reserve"
3    URL      "http://dlq-svc:8080/store"
4    URL      "http://fulfill-svc:8080/fulfill"
```

#### Target Lists

```
Idx  Count  Indices
───  ─────  ───────
0    2      [1, 2]   # fraud, inventory
```

#### Instructions

```
#  n:validate {TimeoutMs:500}
0:  NEXT     flags=HAS_TIMEOUT  arg1=500  arg2=0  arg3=0
                    ^ timeout=500 encoded via arg1 per shift when HAS_TIMEOUT set

Actually: let me be precise. When HAS_TIMEOUT is set:

For NEXT:
  arg1 = timeout_ms
  arg2 = url_cp_idx
  arg3 = unused

So:
0:  opcode=NEXT  flags=0x01 (HAS_TIMEOUT)  arg1=500  arg2=0  arg3=0

#  p:fraud,inventory {TimeoutMs:1000}
1:  opcode=PARALLEL  flags=0x01 (HAS_TIMEOUT)  arg1=1000  arg2=0  arg3=0

#  c
2:  opcode=COLLECT  flags=0  arg1=0  arg2=0  arg3=0

#  f:dlq
3:  opcode=FALLBACK  flags=0  arg1=3  arg2=0  arg3=0

#  n:fulfill
4:  opcode=NEXT  flags=0  arg1=4  arg2=0  arg3=0
```

### 9.2 DSL: `g:amount>10000 n:manual-review | t500 n:auto-approve f:review-queue`

#### Constant Pool

```
Idx  Type         Value
───  ────         ─────
0    FIELD_PATH   "amount"
1    OPERATOR     ">"
2    VALUE        "10000"
3    URL          "http://review-svc:8080/queue"
4    URL          "http://approve-svc:8080/approve"
5    URL          "http://review-svc:8080/fallback"
```

#### Instructions

```
#  g:amount>10000
0:  opcode=GATE  flags=0  arg1=0  arg2=1  arg3=2

#  n:manual-review
1:  opcode=NEXT  flags=0  arg1=3  arg2=0  arg3=0

#  |
2:  opcode=PIPE  flags=0  arg1=0  arg2=0  arg3=0

#  t500 n:auto-approve
3:  opcode=NEXT  flags=0x01 (HAS_TIMEOUT)  arg1=500  arg2=4  arg3=0

#  f:review-queue
4:  opcode=FALLBACK  flags=0  arg1=5  arg2=0  arg3=0
```

---

## 10. Compiler Requirements

### 10.1 Validation

- Constant pool must not contain duplicate URL entries.
- Target list indices must reference valid constant pool indices of type URL.
- Instruction indices in JUMP/JUMP_IF/JUMP_IFN must be in range.
- Map expression CP indices must reference valid entries.
- PIPE must only appear after GATE (enforced by parser, encoded in bytecode).
- No instruction after DROP is reachable (unreachable code may be omitted).

### 10.2 Optimization

- Consecutive EMIT instructions merged into single PARALLEL-style list.
- Unreachable code after DROP is removed.
- Timeout hoisted into NEXT/PARALLEL via HAS_TIMEOUT flag.
- Retry count hoisted into NEXT via HAS_RETRY flag.
- Unused constant pool entries are eliminated.
- Constant pool deduplication (same URL string stored once).

---

## 11. Runtime ABI

### 11.1 Entry Point

The first instruction (index 0) is always the entry point.
No function calls. No return addresses. Linear execution with jumps.

### 11.2 Message Interface

The runtime must provide:

```c
// Runtime provides:
bytes   msg_body();                    // current message body
void    msg_set_body(bytes b);         // replace body (e.g. after MAP)
void    msg_set_failed(bool v);        // set failure flag
bool    msg_failed();                  // read failure flag
int     msg_hop_count();              // current hop count
void    msg_set_partition_key(str k); // set ordering key

// Runtime provides for transport:
bytes   http_call(str url, bytes body, int timeout_ms);
void    http_emit(str url, bytes body);
str[]   resolve_target(str name);     // name → URL resolution
```

### 11.3 Plugin ABI (WASM)

WASI-based interface for plugins:

```wat
;; Plugin receives:
;;   (i32) body_ptr     - pointer to message body in linear memory
;;   (i32) body_len     - body length
;;   (i32) ctx_ptr      - pointer to context struct

;; Plugin returns:
;;   (i32) result_code  - 0=ok, 1=fail, 2=drop
;;   (i32) output_ptr   - pointer to transformed body
;;   (i32) output_len   - transformed body length

(func $transform (export "flowrule_transform")
  (param $body_ptr i32) (param $body_len i32)
  (param $ctx_ptr i32)
  (result i32 i32 i32)
  ...
)
```

---

## 12. File Extension and MIME

| Property          | Value               |
|-------------------|---------------------|
| File extension    | `.flow`             |
| MIME type         | `application/x-flowrule-bytecode` |
| Magic bytes       | `46 4C 4F 57`       |

---

## 13. Version Compatibility

| Bytecode Version | Min Runtime Version | Notes                     |
|------------------|-------------------|---------------------------|
| 1.0              | 1.0               | Initial stable format     |

Runtime must reject bytecode with major version > runtime major version.
Minor version mismatch: runtime may accept (backward compatible).

---

## 14. Future Extensions

Reserved opcode ranges for future versions:

```
0x12–0x1F  Control flow   (LOOP, CALL, RET, SWITCH, MATCH)
0x20–0x2F  Data           (PUSH, POP, LOAD, STORE, MOV)
0x30–0x3F  Math           (ADD, SUB, CMP, AND, OR, XOR)
0x40–0x4F  Plugin         (INVOKE, INVOKE_ASYNC)
0x50–0x5F  AI/ML          (LLM_DECIDE, VECTOR_SEARCH, EMBED)
0x60–0x6F  Scheduler      (YIELD, DELAY, PRIORITY_SET)
0x70–0x7F  Reserved       (future use)
```

---

## Appendix A: Full Instruction Encoding Reference

```
Opcode     Encoding (opcode, flags, arg1, arg2, arg3)
─────────────────────────────────────────────────────
NOP        01 00 0000 00000000 00000000 00000000

NEXT       02 FF 0000 tttttttt uuuuuuuu 00000000
           F=HAS_TIMEOUT: arg1=timeout, arg2=url_cp
           F=HAS_RETRY:   arg2=retry_n, arg3=url_cp
           F=both:        arg1=timeout, arg2=retry_n, arg3=url_cp
           F=none:        arg1=url_cp, arg2=unused, arg3=unused

PARALLEL   03 FF 0000 tttttttt llllllll 00000000
           F=HAS_TIMEOUT: arg1=timeout, arg2=list_idx
           F=none:        arg1=list_idx

COLLECT    04 00 0000 00000000 00000000 00000000

FALLBACK   05 00 0000 uuuuuuuu 00000000 00000000

GATE       06 00 0000 ffffffff oooooooo vvvvvvvv

PIPE       07 00 0000 00000000 00000000 00000000

EMIT       08 00 0000 llllllll 00000000 00000000

DROP       09 00 0000 00000000 00000000 00000000

MAP        0A 00 0000 mmmmmmmm 00000000 00000000

KEY        0B 00 0000 ffffffff 00000000 00000000

SPLIT      0C 00 0000 ffffffff 00000000 00000000

BUFFER     0D 00 0000 cccccccc 00000000 00000000

JUMP       0F 00 0000 iiiiiiii 00000000 00000000

JUMP_IF    10 00 0000 iiiiiiii 00000000 00000000

JUMP_IFN   11 00 0000 iiiiiiii 00000000 00000000

Key:
  t = timeout  u = url_cp      l = list_idx
  f = field_cp o = op_cp       v = value_cp
  m = map_idx  c = count       i = instr_idx
  F = flags byte
```

---

## Appendix B: Example Serialized Module

Hex dump of the module from §9.1 (pipeline `t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill`):

```
0000: 46 4C 4F 57 01 00 00 00  01 00 00 00 00 00 00 00  | FLOW............|
0010: 05 00 00 00 00 00 00 00  05 00 00 00 00 00 00 00  | ................|
0020: 02 00 00 00 00 00 00 00  01 00 00 00 28 00 00 00  | ............(...|
0030: 41 00 00 00 02 00 00 00  69 00 00 00 1C 00 00 00  | A.......i.......|
0040: 03 00 00 00 85 00 00 00  50 00 00 00 04 00 00 00  | ........P.......|
0050: 05 00 00 00 D5 00 00 00  04 00 00 00              | ..............  |

[ ... full hex dump elided for brevity ... ]
```

---

*End of FlowRule Bytecode Specification v1.0*
