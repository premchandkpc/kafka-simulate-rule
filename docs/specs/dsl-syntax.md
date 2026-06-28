# DSL Syntax Specification

## Overview

A compact, single-line DSL for defining Kafka message routing pipelines. The compiler compiles DSL → AST → optimized AST → bytecode `ExecutionPlan`.

## Pipeline Structure

A pipeline is a sequence of **operations** separated by spaces:

```
[t<timeout>] <operations...>
```

## Operations

| Op | Syntax | Description |
|----|--------|-------------|
| **Next** | `n:<service>` | Forward message to service |
| **Async** | `n:<service> async` | Fire-and-forget with no wait |
| **Buffer** | `n:<service> buffer` | Non-blocking send; no retry |
| **Parallel** | `p:<svc1>,<svc2>,...` | Fan-out to multiple services |
| **Collect** | `c` | Collect parallel results into JSON array |
| **Fallback** | `f:<service>` | Route on failure |
| **Gate** | `g:<field><op><value>` | Conditional jump |
| **Split** | `s` or `split` | Split array message into individual records |
| **Map** | `m:<dest>:<src>` or `m:<dest>=<expr>` | Transform message fields |
| **Emit** | `e:<svc1>,<svc2>` | Fire-and-forget publish |
| **Drop** | `d` or `drop` | Halt processing (dead end) |
| **Key** | `k:<expr>` | Routing key expression |
| **Pipe** | `\|` | Pass-thru (nop, removed by optimizer) |
| **Timeout** | `t<ms>` | Set/hoist timeout for subsequent calls |
| **Retry** | `r<N>:<strategy>` | Attach retry policy to preceding call |
| **Chunk** | `chunk:<N>:<mode>` | Split payload into chunks |
| **DAG** | `dag:{<edges>}` | Directed acyclic graph routing |
| **Label** | `<name>:` | Target for Jmp |
| **Jmp** | `j:<label>` | Unconditional jump |

## Detailed Reference

### Next (Service Call)

```
n:<service_name>
```

Synchronous call to a named service. Waits for response.

### Async

```
n:<service_name> async
```

Non-blocking call; execution continues immediately. Can also be written as two operations.

### Buffer

```
n:<service_name> buffer
```

Non-blocking buffered send. No retry semantics.

### Timeout

```
t<milliseconds>
```

Sets the timeout for the next service call. The optimizer hoists timeouts to precede their associated `n:` operation.

**Examples:**
```
t500 n:validate
t1000 n:ship
```

### Retry

```
r<N>:<strategy>[:<param>]
```

Attaches a retry policy to the preceding service call. Must directly follow a Next or Fallback.

**Strategies:**
| Strategy | Syntax | Behavior |
|----------|--------|----------|
| Exponential | `r3:exp` | 2^x backoff (1s, 2s, 4s) |
| Fixed | `r3:fixed:200` | Fixed 200ms interval |
| Linear | `r3:lin:500` | Linear 500ms, 1000ms, 1500ms |

**Examples:**
```
n:validate r3:exp
n:payment r3:fixed:100
n:retry-svc r5:lin:200
```

### Gate (Conditional Branch)

```
g:<field><operator><value> <on-true> [f:<on-false>]
```

Evaluates a JSON field against a value. On match, executes the next operation; on failure, jumps to Fallback.

**Operators:**
| Op | Meaning |
|----|---------|
| `==` | Equal |
| `!=` | Not equal |
| `>` | Greater than |
| `<` | Less than |
| `>=` | Greater or equal |
| `<=` | Less or equal |
| `~` | Contains (substring) |

**Field paths** support dotted navigation:
```
g:user.role==admin n:admin-panel f:user-panel
g:amount>10000 n:manual-review f:auto-approve
g:tags~urgent n:priority-queue
```

### Pipe

```
<operation1> | <operation2>
```

A no-op separator. The optimizer removes pipe nodes, merging adjacent operations. Present in the language for readability.

### Parallel / Collect

```
p:<svc_a>,<svc_b>,<svc_c> c
```

Fan-out to multiple services in parallel. `c` (collect) merges all responses into a JSON array under the `_parallel` field.

**Validation:** `c` must immediately follow a Parallel. Error otherwise.

### Fallback

```
f:<service_name>
```

Executed when the preceding operation fails (timeout, error, etc.). Must follow a service call or gate.

**Examples:**
```
n:validate f:error-queue
g:amount>10000 n:manual f:auto-reject
```

### Split

```
s
```

If the message body is a JSON array, split it into individual messages and process each independently. The array must contain only objects.

**Example:**
```
s n:process-each e:results
```

### Map (Field Transformation)

```
m:<dest>=<expression>
m:<dest>:<source_field>
```

Two modes:

**Copy mode:** `m:target.field:source.field` — copies a value from source path to destination path.

**Expression mode:** `m:target.field=expr` — evaluates an expression and assigns the result.

**Expressions:**
| Form | Example |
|------|---------|
| Field path | `m:x=.user.name` |
| String literal | `m:x='hello'` |
| Concatenation | `m:x=.first + ' ' + .last` |
| Function call | `m:x=uuid()` |

**Built-in functions:**

| Function | Description |
|----------|-------------|
| `uuid()` | Generate UUID v4 |
| `now()` | Current ISO timestamp |
| `lower(s)` | Lowercase string |
| `upper(s)` | Uppercase string |
| `trim(s)` | Trim whitespace |
| `length(s)` | String length |
| `concat(a, b)` | Concatenate strings |
| `base64(s)` | Base64 encode |
| `json(s)` | Parse JSON string |
| `substring(s, start, end)` | Substring |
| `replace(s, from, to)` | String replace |

**Examples:**
```
m:processed_at=now()
m:user_id=.id
m:full_name=.first + ' ' + .last
m:display=upper(.name)
m:email_hash=base64(.email)
m:greeting='hello ' + .name
m:payload=json(.raw_json)
m:text=replace(.body, 'foo', 'bar')
```

### Emit

```
e:<service_a>,<service_b>,...
```

Fire-and-forget publish to one or more services. Non-blocking, no response expected.

**Example:**
```
n:process e:notify,analytics,audit-log
```

### Drop

```
d
```

Terminates pipeline execution immediately. Any operations after Drop are dead code (optimizer removes them). Label targets are preserved even after Drop.

**Example:**
```
g:status==blocked d n:normal-path
```

### Key (Routing Key)

```
k:<expression>
```

Sets or transforms the routing key used for partitioning.

### Chunk

```
chunk:<N>:<mode> <operation>
```

Splits the message into chunks of size N before the subsequent operation.

**Modes:**
| Mode | Description |
|------|-------------|
| `seq` | Process chunks sequentially |
| `par` | Process chunks in parallel |

**Example:**
```
chunk:10:seq n:storage
chunk:50:par n:batch-process
```

### DAG (Directed Acyclic Graph)

```
dag:{<node>: [<dependencies>], ...} e:<output>
```

Declarative DAG routing. Each node is a service; dependencies are listed as a comma-separated list. The DAG executes layer-by-layer: all services in a layer execute in parallel; a service starts only when all its dependencies complete.

**Syntax:**
```
dag:{A:[],B:[A],C:[A],D:[B,C]} e:output
```

This creates:
```
Layer 0: A
Layer 1: B, C (parallel, depend on A)
Layer 2: D (depends on B and C)
```

After all layers complete, results are merged into a JSON object under each node's key. `e:<service>` emits the merged result.

**Validation at compile time:**
- Cycle detection (error on cycles)
- Unknown service references
- Disconnected nodes warning

### Labels and Jumps

```
<label_name>: <operation> j:<label_name>
```

Labels mark a position in the pipeline. Jumps transfer control unconditionally.

**Requirements:**
- Labels must be unique within a pipeline
- Jump targets must exist
- Labels are preserved through optimization (even after dead code removal)

**Example:**
```
start: n:auth g:role==admin n:admin-panel j:end n:user-panel end: e:done
```

## Full Pipeline Examples

**Simple validation and routing:**
```
t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics
```

**Gate with retry:**
```
g:amount>10000 n:manual-review r3:exp f:auto-reject
```

**DAG with downstream emit:**
```
dag:{enrich:[],validate:[enrich],store:[validate]} e:audit-log
```

**Split, process, collect:**
```
s n:enrich g:type==order n:order-pipeline n:generic-pipeline c e:results
```

## Error Handling

| Error | Cause |
|-------|-------|
| Empty pipeline | No operations provided |
| Invalid token | Unrecognized syntax |
| Empty operand | Missing service/field name |
| Collect without parallel | `c` not preceded by `p:` |
| Retry without service | `r:` not following a call |
| Unknown service in DAG | Dependency references missing node |
| Cycle detected | DAG has directed cycles |
| Duplicate label | Label name used twice |
| Undefined jump target | `j:` references non-existent label |
