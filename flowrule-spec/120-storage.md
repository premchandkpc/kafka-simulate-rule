# Storage Specification

## 1. Purpose

FlowRule stores two categories of data: durable (dead letter queue, persisted rule configurations) and ephemeral (idempotency state, circuit breaker state, credit balances).

## 2. Dead Letter Queue

### 2.1 Purpose
Durable storage for messages that could not be delivered after exhausting all retry and fallback options.

### 2.2 Entry Schema
```go
type DLQEntry struct {
    ID        string    // unique entry ID
    RuleID    string    // originating rule
    Target    string    // failed target
    Error     string    // error message
    Retries   int       // retry count attempted
    Body      []byte    // message body at failure
    Headers   map[string]string
    Timestamp time.Time
    ExpiresAt time.Time  // TTL-based eviction
}
```

### 2.3 Storage
- Backend: Badger (embedded LSM tree) for v1
- Default TTL: 72 hours
- Eviction: background GC based on `ExpiresAt`

### 2.4 Snapshot API
```go
type DLQSnapshot struct {
    Entries    []DLQEntry
    TotalCount int
    NextCursor string  // for pagination
}
```

### 2.5 Operations
- `Push(entry)`: write entry
- `List(cursor, limit)`: paginated read
- `Get(id)`: single entry lookup
- `Delete(id)`: manual removal
- `Replay(id, target)`: re-deliver entry
- `Count()`: total entries

## 3. Idempotency Store

### 3.1 Purpose
Prevent duplicate processing of the same message.

### 3.2 Implementation
- In-memory `map[uint64]time.Time` (message ID → timestamp)
- TTL: 5 minutes (configurable)
- Eviction: periodic sweep of expired entries
- Not persisted across restarts (at-most-once within window)

### 3.3 Operations
- `CheckAndSet(id)`: returns true if new, false if duplicate
- `Evict()`: remove expired entries

## 4. Rule Configuration Storage

### 4.1 Format
YAML file with rule definitions:
```yaml
rules:
  - id: "order-routing"
    version: 1
    source: "g:amount>10000 n:manual | n:auto"
    targets:
      manual: "http://manual-review:8080"
      auto: "http://auto-approve:8080"
```

### 4.2 File Watching
- fsnotify-based hot reload
- Debounce: 100ms window
- Atomic replacement: rename or write + swap
- Error handling: failed reload keeps previous rules

## 5. Circuit Breaker State

### 5.1 Purpose
Track failure counts and state transitions per target.

### 5.2 Implementation
- In-memory, per-target state machine
- Not persisted across restarts
- State: CLOSED, OPEN, HALF-OPEN
- Counters: failure count, success count (half-open)
- Timers: last failure time, open timeout

## 6. Credit State

### 6.1 Purpose
Track per-target credit balances.

### 6.2 Implementation
- In-memory, per-target atomic uint64
- Not persisted across restarts
- Balance must never go negative
- Periodic credit refresh (optional)
