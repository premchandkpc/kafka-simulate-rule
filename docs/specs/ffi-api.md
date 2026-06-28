# C FFI API Specification

## Overview

The Rust core exposes a C-compatible FFI interface for the Go I/O shell to call. All functions use `extern "C"` with C-compatible types.

## Memory Ownership Convention

- **`*mut c_char`** (strings): Caller allocates via `flowrule_msg_alloc`; callee reads. Freed via `flowrule_msg_release`.
- **`*mut ExecutionPlan`** (compiled plan): Returned by `flowrule_compile`; caller must free via `flowrule_plan_release`.
- All functions return `i32` status codes (0 = success, negative = error).

## Functions

### Compilation

```c
// Compile a DSL string into an ExecutionPlan
// Returns 0 on success, negative on error
// On success, *out_plan is set to a heap-allocated ExecutionPlan
int flowrule_compile(
    const char* dsl_input,
    struct ExecutionPlan** out_plan
);
```

### Execution

```c
// Execute a compiled plan against a message body
// body is modified in-place with the result
// Returns 0 on success, negative on error
int flowrule_execute(
    struct ExecutionPlan* plan,
    char* body          // In: input JSON, Out: result JSON
);
```

### Message Memory Management

```c
// Allocate a message buffer from the slab pool
// Returns pointer to buffer, or NULL if allocation fails
char* flowrule_msg_alloc(size_t size);

// Release a message buffer back to the slab pool
void flowrule_msg_release(char* ptr);
```

### Plan Lifecycle

```c
// Free a heap-allocated ExecutionPlan
void flowrule_plan_release(struct ExecutionPlan* plan);
```

### String Interning

```c
// Intern a string, returning its ID
// Creates entry if not already present
int flowrule_intern(const char* str);

// Look up an interned string by ID
// Returns pointer to string, or NULL if ID not found
const char* flowrule_intern_lookup(int id);
```

## Data Structures (C View)

```c
// Opaque handle; actual layout is Rust-internal
struct ExecutionPlan;

// Status codes
#define FLOWRULE_OK        0
#define FLOWRULE_ERR_PARSE -1
#define FLOWRULE_ERR_COMPILE -2
#define FLOWRULE_ERR_EXEC   -3
#define FLOWRULE_ERR_NOMEM  -4
```

## Integration with Go

### Expected Go cgo Binding (not yet implemented)

The Go layer `go/internal/bridge/` will use cgo to call these functions:

```go
/*
#cgo LDFLAGS: -L${SRCDIR}/../../../rust/target/release -lflowrule_core
#include "flowrule.h"
*/
import "C"
```

Go will:
1. Accept HTTP/gRPC/ Kafka messages
2. Compile DSL → `ExecutionPlan` (once per rule, cached)
3. Call `flowrule_execute` for each message
4. Route result to output topic/sink

### Thread Safety

- `flowrule_execute` is safe to call concurrently on different plans
- `flowrule_compile` is safe to call concurrently (read-only lookup for intern pool)
- `flowrule_msg_alloc` / `flowrule_msg_release` use lock-free slab pools
