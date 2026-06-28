# Kafka Semantics Specification

## Status: Planned

This document specifies how the flowrule runtime interacts with Kafka. The Go transport layer is responsible for all Kafka concerns; the Rust VM is transport-agnostic.

---

## Consumer Groups

```
flowrule
└── consumer-group
    ├── partition-0 ── worker-0 ── rule-vm
    ├── partition-1 ── worker-1 ── rule-vm
    └── partition-2 ── worker-2 ── rule-vm
```

- One consumer group per deployment
- Each partition is assigned to exactly one worker goroutine
- Workers are long-lived (no per-message goroutine churn)
- Rebalance listener pauses/resumes partition workers

### Partition Assignment

| Strategy | When |
|----------|------|
| Range (default) | Steady state |
| Cooperative Sticky | After rebalance |
| Custom (rack-aware) | Multi-datacenter |

### Rebalance Handling

```
RebalanceTriggered
    ↓
PauseWorkers
    ↓
StorePendingOffsets
    ↓
RevokePartitions
    ↓
AssignPartitions
    ↓
ResumeWorkers
```

1. On rebalance, pause partition workers (drain in-flight)
2. Commit pending offsets
3. Accept new partition assignment
4. Resume processing

---

## Offset Commit

### Commit Strategies

| Mode | Behavior | Use Case |
|------|----------|----------|
| `at-least-once` | Commit after VM execution succeeds | Default — safe |
| `exactly-once` | Commit in the same transaction as the output produce | Transactional |
| `manual` | User controls commit via admin API | Debug, replay |

### Commit Timing

```
Message Received
    ↓
Execute Rule (Rust VM)
    ↓
Produce Output
    ↓
Commit Offset
    ↓
Next Message
```

- Commit interval: configurable (default 500ms or 1000 messages)
- Async commit: commit in background, don't block message processing
- On shutdown: flush pending commits before exiting

### Sticky Commit Queue

```
offsets = [0, 1, 2, 3, 4, 5]

committed = 2
processed = [✓, ✓, ✓, ✓, ✓, ✗]
                              ↑ last committed = 5

On restart: resume from offset 2 + 1 = 3
```

---

## Batch Poll

```
Consumer.PollBatch(100)
    ↓
[]
    ↓
Batch(100 messages)
    ↓
ExecuteBatch (Rust FFI)
```

- `flowrule_execute_batch(plans[], bodies[], results[])` — single FFI call for N messages
- Batch size configurable (default 100)
- Backpressure: if processing is slow, reduce batch size

---

## Exactly-Once & Idempotency

### Producer Idempotency

- Enable `enable.idempotence=true` on the Kafka producer
- All output topics use idempotent producers
- On retry, same sequence number → broker dedup

### Exactly-Once Semantics (EOS)

Requires Kafka 3.0+ with `isolation.level=read_committed`:

```
transactional producer
    ↓
begin transaction
    ↓
produce output
    ↓
commit offset
    ↓
commit transaction
```

The offset commit and output produce are atomic.

### Transactions

- Transactional ID: `flowrule-{group}-{partition}`
- Transaction timeout: 60s (configurable)
- Abort on VM execution failure
- Dead letter queue entries are produced outside the transaction (always deliver)

---

## Dead Letter Queue (DLQ)

### Poison Message Handling

```
Message → VM → Error
    ↓
Retry count < max?
    ├── Yes → Retry (with backoff)
    └── No  → DLQ
```

### DLQ Topic Format

```json
{
  "original_topic": "input",
  "original_partition": 3,
  "original_offset": 1042,
  "key": "msg-key",
  "body": {"original": "payload"},
  "error": "VM execution: service timeout",
  "retry_count": 3,
  "timestamp": "2026-06-28T12:00:00Z"
}
```

### DLQ Consumer

Separate consumer group (`flowrule-dlq-{group}`) for manual inspection and replay.

---

## Backpressure

### Flow Control

```
Input Rate
    ↓
Channel (buffered)
    ↓
Worker (busy?)
    ├── Yes → backpressure signal → pause poll
    └── No  → process message
```

### Mechanisms

| Level | Mechanism | Trigger |
|-------|-----------|---------|
| Go channel | Block send to full channel | Channel at capacity |
| Kafka consumer | `pause()` partition | Memory threshold exceeded |
| Admin API | `/admin/backpressure` | Manual throttle |

### Memory Guard

- Max in-flight messages: configurable (default 10000)
- Max pending bytes: configurable (default 256MB)
- When exceeded: pause consumer → wait for workers to drain → resume

---

## Retry Topics

### Retry Flow

```
Main Topic
    ↓ (consume)
Worker
    ↓ (VM error + retryable)
Retry Topic (with backoff)
    ↓ (consume after delay)
Worker
    ↓ (VM error + retryable)
DLQ
```

### Retry Topic Naming

```
{input-topic}-retry-{delay}
```

Example: `flowrule-input-retry-30s`, `flowrule-input-retry-5m`

### Retry Backoff

| Attempt | Delay    |
|---------|----------|
| 1       | 10s      |
| 2       | 30s      |
| 3       | 5m       |
| 4       | 30m      |
| 5       | 2h       |
| 6+      | DLQ      |

Backoff is based on Kafka's `timestamp` + consumer seek, not application timers.

---

## Ordering & Partition Affinity

### Per-Partition Ordering

- Messages from the same partition are processed sequentially by the same worker
- No reordering within a partition
- No ordering guarantees across partitions

### Sticky Partition

When producing output, use the same key as the input message to preserve partition affinity:

```
input key "user:42" → partition 3
output key "user:42" → partition 3
```

This ensures all messages for a given entity remain in the same partition downstream.

---

## Outbox Pattern

### Transactional Outbox

```
Rule VM
    ↓
Write to Outbox Table (Postgres)
    ↓
Outbox Reader
    ↓
Produce to Kafka
    ↓
Delete from Outbox
```

Use case: exactly-once delivery without Kafka transactions.

---

## Replay

### API

```
POST /admin/replay
{
  "topic": "flowrule-input",
  "partition": 3,
  "offset": 1000,
  "count": 500,
  "target": "flowrule-output"
}
```

### Implementation

- Seeks consumer to specified offset
- Processes messages without committing offsets (read-only mode)
- Produces results to specified target topic

---

## Health Checks

### Consumer Health

```
GET /health/consumer
{
  "group": "flowrule-group",
  "partitions": [0, 1, 2],
  "lag": [14, 3, 27],
  "state": "running"
}
```

### Producer Health

```
GET /health/producer
{
  "outstanding_produces": 5,
  "errors_1m": 0,
  "rate_1m": 234
}
```
