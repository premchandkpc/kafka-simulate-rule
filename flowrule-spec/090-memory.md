# Memory Specification

## 1. Purpose

FlowRule manages memory explicitly to minimize garbage collection pressure and guarantee predictable allocation per message. The memory subsystem provides bump allocators, slab pools, and string interning.

## 2. Architecture

```
┌────────────────────────────────────────────┐
│             Memory Subsystem                 │
│                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │  Arena   │  │  Slab    │  │  Intern  │  │
│  │ Allocator│  │  Pool    │  │  Table   │  │
│  └──────────┘  └──────────┘  └──────────┘  │
│                                              │
│  Bump alloc   3-tier pool   String→uint16    │
│  Overflow→heap 64B/256B/1K  Header interning │
└──────────────────────────────────────────────┘
```

## 3. Arena Allocator

### 3.1 Purpose
Zero-allocation bump allocator for short-lived objects (message metadata, instruction temporaries).

### 3.2 Interface
```
Alloc(size) → []byte
Free()       // reset bump pointer
Reset()      // reuse underlying buffer
```

### 3.3 Behavior
- Pre-allocates a fixed-size buffer (configurable, default 64KB)
- `Alloc`: bump pointer forward, return slice
- `Free`: reset pointer to start (no individual free)
- Overflow: if request exceeds remaining space, allocate from heap
- Thread-safe: not required (arena is per-worker)

### 3.4 Performance Target
- `Alloc`: <10ns for common sizes
- Zero heap allocations when buffer is sufficient
- No GC pressure

## 4. Slab Pool

### 4.1 Purpose
Fixed-size object pools for frequently allocated types (message structs, instruction scratch buffers).

### 4.2 Size Classes
| Class | Size | Use Case |
|-------|------|----------|
| Small | 64 bytes | Headers, metadata |
| Medium | 256 bytes | Message scratch |
| Large | 1024 bytes | Serialization buffer |

### 4.3 Interface
```
Get(size) → []byte       // return from pool or allocate
Put([]byte)               // return to pool
```

### 4.4 Behavior
- Pre-allocates configurable count per class
- `Get`: return from free list or allocate new
- `Put`: return to free list (never GC'd while pool exists)
- Thread-safe: per-size-class mutex
- Benchmark target: <50ns per Get/Put

## 5. Intern Table

### 5.1 Purpose
String interning for frequently repeated strings (header names, field paths). Maps strings to dense uint16 IDs for efficient comparison and storage.

### 5.2 Interface
```
Intern(string) → uint16     // insert or return existing ID
Lookup(uint16) → string     // reverse lookup
```

### 5.3 Behavior
- Pre-populated with common headers: content-type, content-length, accept, authorization
- Seeded with compile-time known strings
- Thread-safe: RWMutex, read-optimized
- Max capacity: 65535 entries (uint16)
- Collision: guaranteed unique (insert-only)

### 5.4 Performance Target
- `Intern`: <30ns for cached, <100ns for new
- `Lookup`: <10ns
- Zero allocations on repeated interning

## 6. Per-Message Allocation Budget

| Object | Size | Count | Total |
|--------|------|-------|-------|
| Message struct | ~200B | 1 | 200B |
| LastResponse | 1KB | 1 | 1KB |
| Error list | 256B | 1 | 256B |
| Headers | 512B | 1 | 512B |
| Scratch | 256B | 1 | 256B |
| **Total** | | | **~2.2KB** |

Target: <1KB peak on hot path (excluding transport payloads).

## 7. Lifecycle

```
Message arrives
  → Arena.Reset()
  → Slab.Get() for message struct
  → Arena.Alloc() for headers
  → Execute instructions
  → Slab.Put() message struct
  → Arena.Free()
```

Memory is reclaimed per-message. No long-lived per-message allocations.
