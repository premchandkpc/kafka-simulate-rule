# Memory Management Specification

## Overview

Three-layer memory architecture designed for high-throughput message processing with minimal allocation overhead.

```
┌──────────────────┐
│   Slab Pool       │  ← Three-tier pre-allocated buffer pool
│  (2KB / 8KB / 64KB)│
└────────┬─────────┘
         │ acquire/release
┌────────▼─────────┐
│   Arena (Bump)    │  ← Bump allocator for per-message temporaries
│  (bumpalo::Bump)  │
└────────┬─────────┘
         │ allocate
┌────────▼─────────┐
│ String Interner   │  ← Lock-free deduplicated string storage
│  (boxcar + HashMap)│
└──────────────────┘
```

## Slab Pool (`slab.rs`)

Pre-allocated, lock-free message buffer pool.

### Size Classes

| Class | Size | Capacity |
|-------|------|----------|
| Small | 2,048 bytes | Pre-allocated per pool |
| Medium | 8,192 bytes | Pre-allocated per pool |
| Large | 65,536 bytes | Pre-allocated per pool |

### Implementation

```rust
pub struct MessageSlabPool {
    small: SegQueue<Vec<u8>>,
    medium: SegQueue<Vec<u8>>,
    large: SegQueue<Vec<u8>>,
}
```

Uses `crossbeam::SegQueue` (lock-free MPSC queue) for contention-free acquire/release.

### Selection Strategy

```rust
fn acquire(&self, size: usize) -> Vec<u8> {
    let class = if size <= SMALL_SIZE { &self.small }
          else if size <= MEDIUM_SIZE { &self.medium }
          else { &self.large };
    class.pop().unwrap_or_else(|| vec![0; class.capacity()])
}
```

- Requests buffer >= requested size
- Falls back to fresh allocation if pool empty
- Pool recycles buffers via `flowrule_msg_release`

### Global Singleton

```rust
static SLAB_POOL: Lazy<Mutex<MessageSlabPool>> = Lazy::new(|| {
    Mutex::new(MessageSlabPool::new(1024, 512, 128))
});
```

Initial capacity: 1024 small, 512 medium, 128 large buffers.

## Arena (`arena.rs`)

Bump allocator for per-message temporary allocations during execution.

```rust
pub struct Arena {
    inner: bumpalo::Bump,
}
```

### Characteristics

- O(1) allocation (bump pointer)
- Entire arena reset between messages (no individual frees)
- Uses `bumpalo::Bump` internally
- Optional slab-backed fallback for large allocations

### Usage

```rust
let arena = Arena::new();
let s: &str = arena.alloc_str("temp_string");
let v: &[u8] = arena.alloc_slice(&[1, 2, 3]);
```

Reset after each message execution:
```rust
arena.reset();
```

## String Interner (`intern.rs`)

Lock-free concurrent string interning for deduplicated string storage.

### Implementation

```rust
pub struct Interner {
    map: Mutex<HashMap<String, u16>>,
    vec: boxcar::Vec<String>,  // Lock-free grow-only vector
}
```

### Operations

```rust
// Intern a string → returns u16 ID
fn intern(&self, s: &str) -> u16 {
    if let Some(&id) = self.map.lock().get(s) { return id; }
    let id = self.vec.push(s.to_string()) as u16;
    self.map.lock().insert(s.to_string(), id);
    id
}

// Look up ID → returns &str
fn lookup(&self, id: u16) -> Option<&str> {
    self.vec.get(id as usize).map(|s| s.as_str())
}
```

### Characteristics

- O(1) amortized interning
- Lock-free reads via `boxcar::Vec`
- Write lock only on `HashMap` insert
- IDs are stable for the lifetime of the interner
- Used by compiler for constant pool deduplication

## Message Lifecycle

```
                  ┌─────────────────────────────┐
                  │  1. flowrule_msg_alloc(size) │──→ Slab pool returns buffer
                  └─────────────┬───────────────┘
                                │
                  ┌─────────────▼───────────────┐
                  │  2. Write message JSON       │
                  │     into slab buffer         │
                  └─────────────┬───────────────┘
                                │
                  ┌─────────────▼───────────────┐
                  │  3. flowrule_execute(plan,   │
                  │     body)                    │
                  │     ├─ Arena reset           │
                  │     ├─ VM dispatch loop      │
                  │     ├─ Interner lookups      │
                  │     └─ Result in body        │
                  └─────────────┬───────────────┘
                                │
                  ┌─────────────▼───────────────┐
                  │  4. Read result from body    │
                  └─────────────┬───────────────┘
                                │
                  ┌─────────────▼───────────────┐
                  │  5. flowrule_msg_release(body)│──→ Buffer back to slab pool
                  └─────────────────────────────┘
```

## Memory Safety

- All `extern "C"` functions use raw pointers; callee must not read past allocated size
- Slab buffers are zero-initialized on creation
- Arena allocations are !Send (bumpalo) — per-thread arena usage
- Interner is `Sync + Send` — safe to share across threads
