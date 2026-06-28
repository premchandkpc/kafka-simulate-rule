# Plugin ABI Specification

## 1. Purpose

The Plugin ABI defines how user-provided logic extends FlowRule execution via WebAssembly (WASM) modules. Plugins are sandboxed, portable, and language-independent.

## 2. Plugin Attachment Points

| Point | Trigger | Input | Output | Use Case |
|-------|---------|-------|--------|----------|
| GATE | Before gate evaluation | Message body | boolean | Custom condition logic |
| MAP | Before map instruction | Message body | transformed body | Custom transformation |
| PRE_CALL | Before NEXT call | Message body | modified body | Request enrichment |
| POST_CALL | After NEXT response | Response body | modified body | Response transformation |

## 3. WASM Interface

### 3.1 Import (host functions provided to plugin)
```
// None in v1 (plugins are pure functions)
```

### 3.2 Export (plugin functions called by host)
```
// Required: return plugin version
get_abi_version() → i32     // must return 1

// Optional attachment points:
// Return 0 for pass-through (no modification)
// Return 1 to apply modification

gate(body_ptr: i32, body_len: i32) → i32          // 0=deny, 1=allow
map(body_ptr: i32, body_len: i32) → i32            // 0=no change, 1=modified
pre_call(body_ptr: i32, body_len: i32) → i32       // 0=no change, 1=modified
post_call(body_ptr: i32, body_len: i32) → i32      // 0=no change, 1=modified
```

### 3.3 Memory Model
- Plugin has its own linear memory (sandboxed)
- Host writes input to plugin memory at pointer
- Plugin reads input, processes, writes output back
- Host reads output from plugin memory
- Maximum buffer size: 64KB (configurable)

### 3.4 ABI Versioning
- `get_abi_version` must return current version
- Host checks version before calling attachment points
- Version mismatch: plugin disabled with warning
- ABI is additive only within major version

## 4. Plugin Execution Contract

```
function executePlugin(ctx, plugin, payload):
    // 1. Check timeout
    ctx, cancel = withTimeout(ctx, 5s)
    defer cancel()

    // 2. Write payload to plugin memory
    ptr = plugin.alloc(len(payload))
    plugin.memory[ptr:ptr+len(payload)] = payload

    // 3. Call WASM function
    result = plugin.call("map", ptr, len(payload))

    // 4. Read result
    if result == 1:
        outputLen = plugin.getOutputLen()
        output = plugin.memory[ptr:ptr+outputLen]
        return output
    return payload  // pass-through
```

## 5. Resource Limits

| Resource | Limit | Enforcement |
|----------|-------|-------------|
| Execution time | 5s | Context deadline |
| Memory | 64KB | WASM linear memory |
| Instructions | 1M | WASM instruction budget (future) |
| Allocations | 1KB per call | WASM memory limit |
| Stack depth | 256 | WASM call stack |

## 6. Plugin Lifecycle

```
Load
  → Read WASM binary
  → Instantiate WASM module
  → Verify ABI version
  → Cache compiled module
  → Register with engine

Execute
  → Match plugin by type + target
  → Invoke with timeout
  → Return result or error

Unload
  → Remove from engine
  → Drop from cache
```

## 7. Security

### 7.1 Sandbox
- No access to host file system
- No access to host network
- No access to host environment variables
- No system calls (WASI restricted)
- Memory isolated per instance

### 7.2 Capability Model (future)
Plugins explicitly declare required capabilities:
- `http:call` — make HTTP requests
- `kv:read` — read from key-value store
- `log:write` — write to log

## 8. Language Support

| Language | Compile Target | Status |
|----------|---------------|--------|
| Rust | wasm32-wasi | Reference |
| TinyGo | wasm32-wasi | Supported |
| AssemblyScript | wasm32 | Planned |
| C | wasm32-wasi | Supported |
| Zig | wasm32-wasi | Planned |

## 9. Performance Target

- Plugin invocation overhead: <50μs (excluding WASM execution)
- Module load: <10ms
- Memory: <1MB per loaded plugin
