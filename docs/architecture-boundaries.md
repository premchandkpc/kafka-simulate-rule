# FlowRule scope and boundaries

FlowRule is a rule engine, not a workflow engine. It answers one question:

> Given this event, what decision and effects should happen?

Everything else — aggregation, timers, long-running coordination, human approval — belongs in application workflow or event choreography.

## Capability matrix

| Capability | FlowRule? | How |
|---|---|---|
| Single event → rules | ✅ | Core |
| Multiple rules per set | ✅ | Rule set with priorities |
| First-match evaluation | ✅ | `mode: first_match` |
| All-matching evaluation | ✅ | `mode: all_matches` |
| Parallel independent rules | ✅ | `all_matches` mode |
| Rule → emit event | ✅ | `EmitAction` via outbox |
| Event → another rule set | ✅ | Event type mapping (`event.type → rule_set`) |
| Command → external service | ✅ | `CommandAction` via `EffectSender` |
| Deterministic evaluation | ✅ | Pure function, no I/O |
| Event aggregation | ⚠️ | External state processor |
| Batch/window processing | ⚠️ | External stream processor |
| Long-running workflow | ❌ | Workflow engine |
| Timers/scheduling | ❌ | Scheduler layer |
| Human approval | ❌ | Application workflow |
| Compensation/saga | ❌ | Workflow/event choreography |
| Arbitrary loops | ❌ | Not in DSL |

## The two action types enforce the boundary

```text
Action
   |
   +-- EmitAction      → produces event → broker → another rule set (internal)
   |
   +-- CommandAction   → sends to external service (workflow layer)
```

`EmitAction` keeps chaining inside FlowRule. `CommandAction` pushes coordination outside.

## Event chaining example

```text
order.created
      |
      v
┌─────────────┐
│ Order Rules │
└──────┬──────┘
       |
       v
EmitAction: fraud.check.requested
       |
       v
  Broker (NATS)
       |
       v
┌─────────────┐
│ Fraud Rules │
└──────┬──────┘
       |
       v
EmitAction: fraud.approved
```

FlowRule does not call the fraud rules directly. It emits an event. The normal event routing causes the second rule set to execute. This is clean choreography, not orchestration.

## What stays outside FlowRule

### Event aggregation

When multiple events must arrive before a decision:

```text
order.created ──────┐
                    │
payment.completed ──┼──► Order Aggregator
                    │
inventory.reserved ─┘
                         │
                         ▼
                   order.ready
                         │
                         ▼
                    FlowRule
```

The aggregator is a separate state processor. It emits a derived event (`order.ready`) that FlowRule evaluates.

### Batch/window processing

When events must be collected over time:

```text
transaction events (1000s)
      |
      v
Stream Processor (5-min window)
      |
      v
customer.transaction.summary
      |
      v
FlowRule
```

### Long-running workflows

When a process spans minutes, hours, or days:

```text
order.approved
      |
      v
Workflow Engine
      |
      +-- schedule payment (day 0)
      +-- wait for shipment (day 1)
      +-- send follow-up (day 7)
```

FlowRule makes the initial decision. The workflow engine handles coordination.

## The principle

```text
Rule:     "Given this snapshot, what should happen?"
Choreography: "What happens next?"
Aggregator:   "Which events belong together?"
Workflow:     "How do I coordinate over time?"
```

Don't make "rule" mean "everything." Keep FlowRule small, deterministic, scalable, and debuggable.
