# FlowRule Internal Architecture Map

## Current State Analysis (as of inspection)

This document maps the existing repository structure against the target 3-service architecture.

---

## 1. EXISTING SERVICE BOUNDARIES

### API (Control Plane) - `cmd/api/`
**Current Responsibilities:**
- Rule compilation and activation: `POST /v1/rules/{ruleSet}/revisions`
- Rule activation: `POST /v1/rules/{ruleSet}/revisions/{revision}/activate` (NOT IMPLEMENTED)
- Execution query: `GET /v1/executions/{executionID}`
- Health check: `GET /health`

**Missing (per target architecture):**
- Workflow CRUD APIs
- Contract CRUD APIs  
- Code generation endpoints
- Batch query APIs
- Quarantine query APIs
- Scheduled event APIs
- Analysis/lineage APIs

### Worker (Data Plane) - `cmd/worker/`
**Current Responsibilities:**
- Event consumption from NATS JetStream
- Shard lease management with fencing tokens
- KeyQueue for per-partition-key ordering
- Event processing via EventService (inbox, evaluation, execution, outbox)
- Effect publishing via EffectService (claim, send, retry, quarantine)
- Batch processing via BatchService
- Scheduler for delayed events
- Observability (metrics, tracing, logging)

**Missing (per target architecture):**
- None significant - Worker already implements most data plane responsibilities

### UI (Control Plane + Observability)
**Current State:** NOT IMPLEMENTED

---

## 2. DOMAIN MODEL MAP

### Core Entities (internal/domain/types.go)

| Entity | Status | Used By |
|--------|--------|---------|
| EventEnvelope | ✅ Complete | API, Worker, Services |
| RuleRevision | ✅ Complete | API, Worker, Services |
| CompiledRule | ✅ Complete | Compiler, Evaluator |
| Predicate | ✅ Complete | Compiler, Evaluator |
| Action (Emit/Command) | ✅ Complete | Compiler, Evaluator, Router |
| Effect | ✅ Complete | Evaluator, EffectService |
| Decision | ✅ Complete | Evaluator, EventService |
| Execution | ✅ Complete | EventService, API |
| OutboxEffect | ✅ Complete | EventService, EffectService |
| InboxEntry | ✅ Complete | EventService |
| ShardLease | ✅ Complete | LeaseManager, Scheduler |
| QuarantineEntry | ✅ Complete | EventService, EffectService |
| RuleActivation | ✅ Complete | RuleService, EventService |
| ScheduledEvent | ✅ Complete | Scheduler |
| WorkflowInstance | ✅ Complete | (Not fully wired) |
| BatchRun / BatchData | ✅ Complete | BatchService |

### Domain Invariants (Enforced)
- ✅ Event identity: `tenant_id + event_id` (Inbox unique constraint)
- ✅ Rule revision identity: `tenant_scope + rule_set + revision` (PK)
- ✅ Activation: One active revision per `tenant_scope + rule_set` (PK + FK)
- ✅ Execution pins: event, tenant, rule_set, revision, decision_hash
- ✅ Effect ID deterministic: `tenant|event|rule_set|revision|rule_id|action_index`
- ✅ Outbox states: pending → claimed → delivered/quarantined (with retry)
- ✅ Execution states: pending → completed/failed/quarantined

---

## 3. PORT INTERFACES MAP (internal/ports/)

### Inbound Ports (Services)
| Interface | Status | Implementations |
|-----------|--------|-----------------|
| EventProcessor | ✅ | events.Service |
| EffectPublisher | ✅ | effects.Service |
| RuleCompiler | ✅ | rules.Compiler |
| RuleEvaluator | ✅ | rules.Evaluator |
| RuleService | ✅ (partial) | rules.Service |
| EffectService | ✅ (partial) | effects.Service |
| WorkflowService | ✅ (interface only) | NOT IMPLEMENTED |
| BatchService | ✅ (interface only) | batches.Service |
| SchedulerService | ✅ (interface only) | scheduler.Scheduler |
| ShardService | ✅ (interface only) | shard.LeaseManager |
| QuarantineService | ✅ (interface only) | NOT IMPLEMENTED |

### Outbound Ports (Repositories)
| Interface | SQL Adapter | Mongo Adapter | Memory Adapter |
|-----------|-------------|---------------|----------------|
| Transaction | ✅ | ✅ | N/A |
| EventRepository | ✅ | ✅ | N/A |
| InboxRepository | ✅ | ✅ | ✅ |
| ExecutionRepository | ✅ | ✅ | ✅ |
| OutboxRepository | ✅ | ✅ | N/A |
| RuleRepository | ✅ | ✅ | N/A |
| ActivationRepository | ✅ | ✅ | N/A |
| WorkflowRepository | ✅ | ✅ | N/A |
| ScheduledEventRepository | ✅ | ✅ | N/A |
| BatchRepository | ✅ | ✅ | N/A |
| ShardLeaseRepository | ✅ | ✅ | N/A |
| QuarantineRepository | ✅ | ✅ | N/A |
| ContractRegistry | N/A | N/A | ✅ |

---

## 4. SERVICE IMPLEMENTATION MAP (internal/services/)

### Rules Service (`internal/services/rules/service.go`)
**Current:** Compile + Activate + GetExecution
**Missing:** ListRevisions, GetRevision, Deactivate, ListActiveRules

### Events Service (`internal/services/events/service.go`)
**Current:** Process + QuarantineEvent
**Missing:** GetExecution, ListExecutions (defined in ports but not implemented)

### Effects Service (`internal/services/effects/service.go`)
**Current:** PublishBatch (claim + send + retry + quarantine)
**Missing:** Dispatch, DispatchBatch, Retry, Quarantine, GetPending, GetByExecution

### Batches Service (`internal/services/batches/service.go`)
**Current:** Tick (batch formation + processing)
**Missing:** CreateBatch, GetBatch, ListBatches

---

## 5. RUNTIME COMPONENTS MAP (internal/runtime/)

| Component | Status | Description |
|-----------|--------|-------------|
| Scheduler | ✅ | Polls scheduled_events, publishes to NATS, marks released |
| KeyQueue | ✅ | Per-partition-key ordering with hot key detection |
| LeaseManager | ✅ | Shard acquisition, renewal, fencing tokens |

---

## 6. ADAPTER IMPLEMENTATION MAP (internal/adapters/)

### Database Adapters
| Adapter | Tables/Collections | Status |
|---------|-------------------|--------|
| SQL (PostgreSQL) | All 12 tables | ✅ Complete with migrations |
| MongoDB | All 12 collections | ✅ Complete |
| Memory | Inbox, ContractRegistry | ✅ Test-only |

### Broker Adapters
| Adapter | Status |
|---------|--------|
| NATS JetStream | ✅ Consumer + Publisher + StreamAdmin |

### Effects Adapters
| Adapter | Status |
|---------|--------|
| Fake | ✅ Test-only |
| HTTP | Not found (interface exists in ports) |

### Observability
| Component | Status |
|-----------|--------|
| Logging (zerolog) | ✅ |
| Metrics (Prometheus) | ✅ |
| Tracing (OpenTelemetry) | ✅ |

---

## 7. DATA FLOW MAP

### Event Processing Flow (Worker)
```
NATS Fetch → KeyQueue → EventService.Process()
    │
    ├── Inbox.Get (dedup check)
    ├── Inbox.Insert (processing)
    ├── Activation.Get (active rule)
    ├── RuleRepo.GetActive (compiled revision)
    ├── Evaluator.Evaluate (pure function)
    ├── Execution.Save (pending)
    ├── Outbox.Insert (effects)
    ├── Inbox.MarkCommitted
    ├── Execution.UpdateStatus (completed)
    └── TX Commit
    │
    └── ACK (only after commit)
```

### Effect Publishing Flow (Async)
```
EffectService.PublishBatch()
    │
    ├── Outbox.ClaimPending (batch)
    ├── For each effect:
    │   ├── EffectSender.Send
    │   ├── On success: Outbox.MarkDelivered
    │   └── On failure: ScheduleRetry / Quarantine
```

### Batch Processing Flow
```
BatchService.Tick()
    │
    ├── BatchRepository.ListUnbatched
    ├── Group by tenant|partition_key|rule_set
    ├── Check ready (accumulate/window)
    ├── Compute deterministic BatchID
    ├── Create BatchData envelope
    ├── EventService.Process (as regular event)
    ├── Save BatchRun
    └── MarkBatched (idempotent)
```

### Scheduler Flow
```
Scheduler.runLoop()
    │
    ├── ScheduledEventRepository.GetDue
    ├── For each event:
    │   ├── Marshal EventEnvelope
    │   ├── PublishToShard (NATS)
    │   └── MarkReleased
```

---

## 8. GAP ANALYSIS AGAINST TARGET ARCHITECTURE

### Critical Gaps (Must Implement)

| Area | Current | Target | Gap |
|------|---------|--------|-----|
| **Workflow API** | Domain + Repo only | Full CRUD + Transitions | No API endpoints, no service implementation |
| **Contract API** | Memory registry only | CRUD + Validation + Codegen | No API, no SQL/Mongo adapters, no codegen |
| **Codegen CLI** | None | `cmd/codegen/` for Go/Java/Protobuf | Missing entirely |
| **Example Apps** | None | `examples/ecommerce/food-delivery/banking` | Missing entirely |
| **UI Service** | None | Control plane + Observability | Missing entirely |

### Important Gaps (Should Implement)

| Area | Current | Target | Gap |
|------|---------|--------|-----|
| **RuleService** | Partial | Full ports.RuleService | Missing: ListRevisions, GetRevision, Deactivate, ListActiveRules |
| **EffectService** | Partial | Full ports.EffectService | Missing: Dispatch, GetPending, GetByExecution |
| **BatchService** | Partial | Full ports.BatchService | Missing: CreateBatch, GetBatch, ListBatches |
| **WorkflowService** | Interface only | Full implementation | Missing service impl |
| **QuarantineService** | Interface only | Full implementation | Missing service impl |
| **Scheduler Concurrency** | Basic | Durable claim/lease | Race condition: GetDue → Publish → MarkReleased |

### Architectural Refinements Needed

| Area | Issue | Recommendation |
|------|-------|----------------|
| **Scheduler** | Concurrent schedulers can duplicate | Add claim/lease before publish |
| **Execution Status** | No CAS/transition validation | Add explicit transition methods |
| **Mongo Transactions** | Used for Inbox+Execution+Outbox | Verify atomicity matches SQL |
| **Tenant Isolation** | Enforced by queries | Add middleware for API auth |

---

## 9. IMPLEMENTATION PRIORITY

### Phase 1: Complete Control Plane APIs (API Service)
1. **Workflow API** - CRUD, transitions, state queries
2. **Contract API** - CRUD, validation, schema registry
3. **Codegen CLI** - Go, Java, Protobuf, JSON Schema generators
4. **Extended Rule API** - ListRevisions, GetRevision, Deactivate, ListActiveRules
5. **Batch/Quarantine/Scheduled APIs** - Query endpoints

### Phase 2: Complete Service Implementations
1. **WorkflowService** implementation
2. **QuarantineService** implementation
3. **BatchService** query methods
4. **Scheduler concurrency fix** - claim before publish

### Phase 3: Example Applications
1. **examples/ecommerce** - Order → Payment → Fraud → Shipment
2. **examples/food-delivery** - Order → Restaurant → Driver → Delivered
3. **examples/banking** - Transaction → Compliance → Settlement

### Phase 4: UI Service (Future)
1. React/TypeScript frontend
2. Rule/Workflow/Contract editors
3. Execution lineage visualization
4. Metrics dashboards

---

## 10. FILES TO CREATE/MODIFY

### New Files Needed
```
cmd/api/
  ├── handlers/
  │   ├── workflow.go       # Workflow HTTP handlers
  │   ├── contract.go       # Contract HTTP handlers
  │   ├── batch.go          # Batch query handlers
  │   ├── quarantine.go     # Quarantine query handlers
  │   └── scheduled.go      # Scheduled event handlers

cmd/codegen/
  ├── main.go               # Codegen CLI entry
  ├── generators/
  │   ├── go.go             # Go type generator
  │   ├── java.go           # Java class generator
  │   ├── protobuf.go       # Protobuf generator
  │   └── jsonschema.go     # JSON Schema generator
  └── contract/
      └── loader.go         # Contract loading/validation

internal/services/
  ├── workflow/
  │   └── service.go        # WorkflowService implementation
  └── quarantine/
      └── service.go        # QuarantineService implementation

internal/adapters/sql/
  ├── contract.go           # SQL ContractRegistry
  └── workflow.go           # (exists)

internal/adapters/mongo/
  ├── contract.go           # Mongo ContractRegistry
  └── workflow.go           # (exists)

examples/
  ├── ecommerce/
  │   ├── contracts/
  │   ├── rules/
  │   ├── workflows/
  │   └── backend/
  ├── food-delivery/
  │   ├── contracts/
  │   ├── rules/
  │   ├── workflows/
  │   └── backend/
  └── banking/
      ├── contracts/
      ├── rules/
      ├── workflows/
      └── backend/

migrations/
  ├── 013_contracts.sql     # Contract schema
  └── 014_workflow_definitions.sql  # Workflow definitions (if needed)
```

### Files to Modify
```
cmd/api/main.go             # Add new route handlers
internal/services/rules/service.go    # Complete RuleService interface
internal/services/batches/service.go  # Add query methods
internal/ports/services.go  # Verify interfaces complete
internal/factory/factory.go # Wire new services
```

---

## 11. CONTRACT MODEL EXTENSION

Current `domain.ContractSchema`:
```go
type ContractSchema struct {
    Name       string
    Version    string
    Fields     map[string]ContractField
    Owner      string
    Deprecated bool
}
```

**Required Extensions for Codegen:**
```go
type ContractSchema struct {
    Name            string
    Version         string
    Namespace       string        // e.g., "com.example.ecommerce"
    Description     string
    Fields          map[string]ContractField
    Compatibility   CompatibilityPolicy  // BACKWARD, FORWARD, FULL, NONE
    Owner           string
    Deprecated      bool
    CreatedAt       time.Time
    UpdatedAt       time.Time
    Metadata        map[string]string
}

type ContractField struct {
    Type        string
    Description string
    Required    bool
    Default     interface{}
    Enum        []interface{}
    Format      string  // e.g., "uuid", "date-time", "email"
    Items       *ContractField  // for arrays
    Properties  map[string]ContractField  // for objects
}
```

---

## 12. WORKFLOW DEFINITION MODEL

**New Domain Type Needed:**
```go
type WorkflowDefinition struct {
    WorkflowType  string
    Version       int64
    States        []WorkflowState
    Transitions   []WorkflowTransition
    Rules         map[string]string  // state -> rule_set
    CreatedAt     time.Time
    UpdatedAt     time.Time
}

type WorkflowState struct {
    Name        string
    Type        StateType  // START, END, INTERMEDIATE
    Rules       []string   // rule sets to evaluate
    OnEnter     []Action   // actions on state entry
    OnExit      []Action   // actions on state exit
}

type WorkflowTransition struct {
    From        string
    To          string
    EventType   string     // event that triggers transition
    Condition   *Predicate // optional guard
}
```

---

## 13. VERIFICATION CHECKLIST

### Domain Invariants to Preserve
- [ ] Event identity: `tenant_id + event_id` unique
- [ ] Rule revision immutability
- [ ] Activation single-writer
- [ ] Execution pins revision + decision_hash
- [ ] Effect ID determinism
- [ ] Outbox at-least-once + idempotency keys
- [ ] Shard ordering via partition_key
- [ ] Fencing token validation on commit

### Architecture Boundaries to Enforce
- [ ] API never processes business events
- [ ] Worker never exposes config management APIs
- [ ] Domain has zero external dependencies
- [ ] Ports define interfaces only
- [ ] Adapters implement ports, no business logic
- [ ] Services orchestrate, don't contain infrastructure

### Test Coverage Targets
- [ ] Unit: Evaluator, Compiler, DecisionHash, EffectID, Workflow transitions
- [ ] Contract: All repository adapters (SQL, Mongo, Memory)
- [ ] Integration: Full event processing, duplicate handling, outbox retry, workflow optimistic locking
- [ ] E2E: Producer → Broker → Worker → Rule → Execution → Outbox → Destination

---

## 14. DECISIONS LOG

| Decision | Rationale |
|----------|-----------|
| Keep SQL + Mongo dual adapters | Demonstrates database-agnostic domain |
| NATS JetStream as primary broker | Built-in persistence, ordering, consumer groups |
| 4096 virtual shards | Fixed mapping, no rebalancing complexity |
| Deterministic effect IDs | Enables destination idempotency without coordination |
| Inbox pattern for dedup | Proven exactly-once at business level |
| Outbox pattern for effects | Reliable delivery without distributed transactions |
| Workflow as runtime (not library) | Single platform executes all business flows |
| Contract-first codegen | Prevents schema drift between services |

---

## 15. NEXT STEPS

1. **Immediate**: Implement Workflow API endpoints in `cmd/api/`
2. **Immediate**: Implement Contract API + SQL/Mongo adapters
3. **Immediate**: Create `cmd/codegen/` with Go/Java/Protobuf generators
4. **Short-term**: Build `examples/ecommerce` with full contract/rule/workflow definitions
5. **Short-term**: Fix scheduler concurrency with claim-based approach
6. **Medium-term**: Implement WorkflowService, QuarantineService
7. **Future**: UI service as separate React/TypeScript application

---

*Generated from repository inspection on $(date). This is a living document.*