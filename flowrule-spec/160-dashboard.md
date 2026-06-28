# Dashboard Specification

## 1. Purpose

The FlowRule Dashboard provides real-time visibility into rule execution, target health, and system performance. Future feature (post-v1).

## 2. Scope (v1)

Not included in v1. All observability data is available via:
- Prometheus metrics (Grafana dashboards)
- OpenTelemetry traces (Jaeger, Tempo)
- Structured logs (Loki, Elastic)
- Event bus (custom subscribers)

## 3. Future Views

### 3.1 Overview
- Messages processed (rate, total)
- Success/failure ratio
- Active rules
- Worker utilization
- P99 latency

### 3.2 Rule Detail
- Per-rule execution stats
- Instruction-level breakdown
- Recent failures
- Hot reload history

### 3.3 Target Health
- Per-target latency
- Circuit breaker state
- Credit balance
- Error rate
- Retry rate

### 3.4 DLQ Browser
- List dead letter entries
- Search by rule, target, error
- Replay single or batch
- Export to JSON

### 3.5 Plugin Inspector
- Loaded plugins
- Execution counts
- Error rate
- Memory usage

## 4. Architecture (future)

```
┌────────────┐     ┌────────────┐     ┌────────────┐
│  FlowRule   │────▶│  Prometheus │────▶│  Grafana    │
│  Runtime    │     │  / OTel    │     │  Dashboard  │
└────────────┘     └────────────┘     └────────────┘
       │                                      │
       │ events                               │ API
       ▼                                      ▼
┌────────────┐                       ┌────────────┐
│  Event Bus │                       │  Frontend  │
│  (internal)│                       │  (React)   │
└────────────┘                       └────────────┘
```

## 5. Tech Stack (future)

- Frontend: TypeScript + React
- Backend: Go (serves API + proxies metrics)
- Charts: D3.js or Chart.js
- Data: Prometheus queries + event bus replay
