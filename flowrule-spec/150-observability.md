# Observability Specification

## 1. Purpose

FlowRule emits three signals: metrics (aggregated), traces (request-scoped), and logs (event-scoped). Every instruction execution, transport call, and lifecycle transition is observable by default.

## 2. Metrics

### 2.1 Counters
| Name | Labels | Description |
|------|--------|-------------|
| `flowrule_messages_total` | rule, status | Messages processed |
| `flowrule_hops_total` | rule, target, status | Instructions executed |
| `flowrule_errors_total` | rule, stage, error | Errors by stage |
| `flowrule_retries_total` | rule, target | Retry attempts |
| `flowrule_circuit_breaker_changes_total` | target, state | Breaker transitions |
| `flowrule_credits_exhausted_total` | target | Backpressure events |

### 2.2 Histograms
| Name | Labels | Description |
|------|--------|-------------|
| `flowrule_execution_duration_ms` | rule, stage | Per-stage latency |
| `flowrule_hop_duration_ms` | target | Per-target latency |
| `flowrule_message_duration_ms` | rule | Total message latency |

### 2.3 Gauges
| Name | Labels | Description |
|------|--------|-------------|
| `flowrule_credits_available` | target | Current credit balance |
| `flowrule_workers_busy` | — | Currently executing workers |
| `flowrule_queue_depth` | — | Pending messages |
| `flowrule_rules_loaded` | — | Active rule count |
| `flowrule_circuit_breaker_state` | target | 0=closed, 1=open, 2=half-open |

## 3. Tracing

### 3.1 Span Model
Each message execution produces a trace:
```
Root: flowrule.execute (message processing)
  ├── flowrule.rule.match (rule matching)
  ├── flowrule.hop (per NEXT/PARALLEL)
  │   ├── flowrule.transport.call (HTTP/gRPC call)
  │   └── flowrule.plugin (if plugin attached)
  └── flowrule.emit (fire-and-forget)
```

### 3.2 Span Attributes
| Attribute | Description |
|-----------|-------------|
| `flowrule.rule_id` | Matched rule ID |
| `flowrule.rule_version` | Rule version |
| `flowrule.message_id` | Message identifier |
| `flowrule.hop_count` | Current hop number |
| `flowrule.target` | Target URL |
| `flowrule.stage` | Current instruction |
| `flowrule.failed` | Whether hop failed |

### 3.3 Propagation
- Trace context extracted from message headers (W3C TraceContext)
- New trace created if no context present
- Propagated to downstream calls via headers

## 4. Logging

### 4.1 Log Levels
| Level | Use |
|-------|-----|
| debug | Instruction dispatch, credit changes |
| info | Rule loaded, engine started/stopped |
| warn | Retry, circuit breaker open, backpressure |
| error | Failed delivery, plugin timeout, compile error |
| fatal | Unrecoverable (engine fails to start) |

### 4.2 Structured Fields
| Field | Description |
|-------|-------------|
| `rule_id` | Active rule |
| `message_id` | Current message |
| `target` | Call target |
| `hop` | Current hop |
| `duration` | Stage duration |
| `error` | Error message |

## 5. Event Bus

Internal pub/sub for lifecycle events:

| Event | Payload | Subscribers |
|-------|---------|-------------|
| `msg.started` | msg_id, rule_id | Monitoring |
| `msg.finished` | msg_id, hops, duration | Analytics |
| `msg.dropped` | msg_id, rule_id | Monitoring |
| `msg.failed` | msg_id, error, stage | Alerting |
| `hop.succeeded` | target, duration | Dashboard |
| `hop.failed` | target, error, retries | Alerting |
| `rule.matched` | rule_id, version | Debug |
| `partition.assigned` | partition_key | Debug |

## 6. Health Endpoints

| Endpoint | Response | Purpose |
|----------|----------|---------|
| `/healthz` | 200 OK | Liveness probe |
| `/readyz` | 200 OK / 503 | Readiness probe |
| `/metrics` | Prometheus text | Metrics scrape |
