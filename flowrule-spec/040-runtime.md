# Runtime Specification

## 1. Purpose

The Runtime manages the lifecycle of message execution: rule matching, worker dispatch, resource governance, and graceful shutdown. It is the entry point for all messages and the owner of the execution context.

## 2. Responsibilities

- Maintain the active rule table (atomic swap for hot reload)
- Match incoming messages to rules
- Dispatch matched messages to the worker pool
- Govern concurrency, backpressure, and resource limits
- Coordinate graceful shutdown (drain in-flight, stop ingress)
- Emit lifecycle events
- Expose health and readiness

## 3. Rule Matching

Rules are evaluated in priority order (lower number = higher priority). First match wins. A rule matches when:

- No condition: always matches
- Simple condition: field == value
- Complex conditions: future extension

Matching is O(n) in rule count. Rule table is `atomic.Pointer[RuleTable]` for lock-free reads.

## 4. Worker Pool

```
┌──────────────────────┐
│     Engine            │
│                       │
│  Match → Submit()    │
│    │                  │
│    ▼                  │
│  ┌──────────────┐    │
│  │  Worker Pool  │    │
│  │  [8 workers]  │    │
│  │               │    │
│  │  ┌─────────┐  │    │
│  │  │ Worker1 │──│───▶│ VM.Execute()
│  │  ├─────────┤  │    │
│  │  │ Worker2 │──│───▶│ VM.Execute()
│  │  ├─────────┤  │    │
│  │  │ ...     │  │    │
│  │  └─────────┘  │    │
│  └──────────────┘    │
└──────────────────────┘
```

### 4.1 Configuration
- `WorkerCount`: number of concurrent goroutines (default: number of CPUs)
- `QueueDepth`: pending message queue per worker (default: 100)
- `CreditsLimit`: per-target credit balance (default: 100)

### 4.2 Behavior
- Workers are goroutines consuming from a channel
- Submit is non-blocking unless queue is full
- Full queue triggers backpressure signal
- Each worker has a context with the message deadline

## 5. Idempotency

Optional idempotency check via `IdempotencyStore` (in-memory map). Messages with duplicate IDs are silently dropped. TTL-based eviction (default: 5 minutes).

## 6. Graceful Shutdown

```
SIGTERM/SIGINT received
  → Stop accepting new messages
  → Drain in-flight (wait for completion or deadline)
  → Close transports
  → Flush DLQ
  → Close event bus
  → Exit
```

Shutdown respects a configurable drain timeout (default: 30s). In-flight messages exceeding this are cancelled.

## 7. Health Endpoints

- `/healthz`: always 200 when running
- `/readyz`: 200 when rule table loaded and workers ready
- `/metrics`: Prometheus scrape endpoint (future)

## 8. Dependencies

| Dependency | Type | Description |
|------------|------|-------------|
| Executor | Interface | VM implementation |
| Rule table | Atomic pointer | Active rules |
| Intern table | Service | String interning |
| DLQ | Interface | Dead letter storage |
| Idempotency store | Service | Duplicate detection |
| Credits | Service | Backpressure |
| Breakers | Service | Circuit breakers |
| Metrics | Service | Prometheus counters |
| Bus | Service | Event bus |
