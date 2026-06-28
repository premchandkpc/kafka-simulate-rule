# Scheduler Specification

## 1. Purpose

The Scheduler governs how messages are assigned to workers, how concurrency is bounded, and how backpressure propagates through the system.

## 2. Architecture

```
┌──────────────────────────────────────────────┐
│                  Scheduler                     │
│                                                │
│  ┌─────────────────┐  ┌───────────────────┐   │
│  │  Worker Pool     │  │  Credit Controller │   │
│  │                  │  │                   │   │
│  │  ┌─────┐  ┌───┐ │  │  TargetA: 85/100  │   │
│  │  │ W1  │  │ W2 │ │  │  TargetB: 100/100│   │
│  │  ├─────┤  ├───┤ │  │  TargetC: 0/100   │   │
│  │  │ W3  │  │...│ │  │                   │   │
│  │  └─────┘  └───┘ │  └───────────────────┘   │
│  └─────────────────┘                            │
│                                                │
│  ┌──────────────────────────────────────────┐  │
│  │         Message Queue (per worker)        │  │
│  │  ┌────┐ ┌────┐ ┌────┐ ┌────┐            │  │
│  │  │ M1 │ │ M2 │ │ M3 │ │ M4 │ ...        │  │
│  │  └────┘ └────┘ └────┘ └────┘            │  │
│  └──────────────────────────────────────────┘  │
└──────────────────────────────────────────────┘
```

## 3. Worker Pool

### 3.1 Model
- Fixed-size pool of goroutines
- Each worker has a buffered channel (inbox)
- Workers are long-lived (created at startup, never destroyed)
- Configurable count: `WorkerCount` (default: runtime.NumCPU)

### 3.2 Dispatch
```
Submit(msg):
    worker = select idle worker (round-robin)
    worker.inbox <- msg
    if all workers busy:
        block until worker available (backpressure)
```

### 3.3 Worker Lifecycle
```
Worker(inbox):
    for msg in inbox:
        engine.execute(msg)
        report metrics
        release credits
```

## 4. Credit Controller

### 4.1 Purpose
Prevent overwhelming downstream targets with more requests than they can handle.

### 4.2 Model
Per-target credit bucket:
- Initial credit: `CreditsLimit` (default 100)
- `CanSend(target)`: returns true if balance > 0
- `Debit(target)`: decrement balance (on send)
- `Credit(target)`: increment balance (on response)

### 4.3 Behavior
- Debit before call attempt
- Credit after response (success or failure)
- Block when balance is zero (backpressure)
- No negative balance (floor at 0)

## 5. Partition Ordering

### 5.1 Purpose
Messages with the same partition key are executed sequentially to preserve order.

### 5.2 Model
- Per-partition mutex (sharded)
- KEY/SPLIT instruction extracts partition key
- Workers acquire partition lock before executing
- Same partition = serial execution
- Different partitions = concurrent execution

### 5.3 Partition Count
Default: 256 shards. Configurable. Trade-off: more shards = more concurrency, more memory.

## 6. Backpressure Propagation

```
Downstream slow
  → Credit balance reaches 0
  → Worker blocks on CanSend check
  → Worker cannot accept new messages
  → Engine blocks on Submit
  → Caller blocks on Ingress call
  → Backpressure propagates to upstream
```

## 7. Fairness

- Round-robin worker selection
- No starvation: each worker has equal chance
- Credit is per-target (one slow target doesn't affect others)
- No priority queues in v1 (all messages equal)
