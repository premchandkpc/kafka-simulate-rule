# Request Flows

## 1. Event Intake Flow

```text
POST /v1/events
```

```text
Client
  |
  v
[API Server] --validate envelope--> [Ingress Handler]
  |
  | 1. Validate JSON schema (id, type, tenant_id, partition_key, occurred_at, data)
  | 2. Compute virtual_shard = hash(tenant_id + ":" + partition_key) mod 4096
  | 3. Publish to JetStream subject "events.{tenant_id}.{type}"
  | 4. Return 202 Accepted with event_id
  |
  v
[JetStream Stream] --durable storage-->
  |
  v
[Worker Fetch Loop] --Fetch()--> [JetStream Consumer]
  |
  | 1. Receive delivery with event payload
  | 2. Deserialize into EventEnvelope
  | 3. Route to ProcessEventUseCase
  |
  v
[ProcessEventUseCase.Execute]
  |
  | BEGIN TRANSACTION
  | 1. Validate envelope (required fields, occurred_at not zero)
  | 2. Check inbox for duplicate (tenant_id, event_id)
  |    - If exists: return existing execution (idempotent)
  | 3. Insert inbox entry (tenant_id, event_id, status='processing')
  |    - If conflict: return existing execution
  | 4. Read activation for (tenant_scope, rule_set=event_type)
  |    - If not found: return ErrRuleNotActive
  | 5. Read active rule revision
  |    - If not found: return ErrRuleNotFound
  | 6. Evaluate(revision, event, facts=nil)
  |    - For each rule (sorted by priority DESC):
  |      - Evaluate predicate tree
  |      - If match: collect Then actions
  |      - If no match: collect Otherwise actions
  |    - Build Decision with matched rules, effects, hash
  | 7. Generate execution_id (UUID)
  | 8. Compute deterministic effect_id for each effect
  |    SHA-256(tenant_id | event_id | rule_set | revision | rule_id | action_index)
  | 9. Insert execution record
  | 10. Insert outbox effects
  | 11. Mark inbox committed (execution_id, committed_at)
  | COMMIT
  | 12. Ack broker delivery (only after commit)
  | 13. Return execution
  |
  v
[Worker] --Ack()--> [JetStream]
  |
  v
[Publisher Loop] --every 5s--> [OutboxRepository.ClaimPending]
  |
  | 1. SELECT ... FOR UPDATE SKIP LOCKED WHERE status='pending' AND available_at <= NOW()
  | 2. For each claimed effect:
  |    - EffectSender.Send(effect)
  |    - On success: MarkDelivered(effect_id)
  |    - On failure: ScheduleRetry or Quarantine
  | 3. Publish to destination with effect_id as idempotency key
  |
  v
[Destination Service]
```

---

## 2. Rule Deployment Flow

```text
POST /v1/rules/{ruleSet}/revisions
Body: { rule_set, revision, mode, rules: [...] }
```

```text
Client
  |
  v
[API Server] --POST /v1/rules/{ruleSet}/revisions--> [ActivateRuleUseCase]
  |
  | 1. Parse JSON body
  | 2. Validate rule_set matches URL path parameter
  |
  v
[Compiler.Compile(source)]
  |
  | 1. Validate JSON structure
  | 2. Check rule_set is non-empty
  | 3. Check revision > 0
  | 4. Check mode is "first_match" or "all_matches"
  | 5. Parse rules array
  | 6. For each rule:
  |    a. Check id is non-empty and unique
  |    b. Check priority is unique across rules
  |    c. Validate predicate tree (operators, paths, nesting depth)
  |    d. Count predicates (max 200)
  |    e. Check nesting depth (max 10)
  |    f. Validate actions (emit or command, not both)
  |    g. Check payload size (max 256KB)
  | 7. Sort rules by priority DESC
  | 8. Compute content_hash = SHA-256(source)
  | 9. Return RuleRevision with compiled rules
  |
  v
[RuleRepository.Save(revision)]
  |
  | INSERT INTO rule_revisions
  |   (tenant_scope, rule_id, revision, source, compiled, content_hash, compiler_version, match_mode)
  | ON CONFLICT DO NOTHING
  |
  v
[ActivationRepository.Set(activation)]
  |
  | INSERT INTO rule_activations
  |   (tenant_scope, rule_set, revision, version, actor, activated_at)
  | ON CONFLICT (tenant_scope, rule_set) DO UPDATE SET
  |   revision = EXCLUDED.revision,
  |   version = version + 1,
  |   actor = EXCLUDED.actor,
  |   activated_at = EXCLUDED.activated_at
  |
  v
Return 200 OK with activation details
```

---

## 3. Rule Activation Flow

```text
POST /v1/rules/{ruleSet}/revisions/{revision}/activate
```

```text
Client
  |
  v
[API Server]
  |
  | 1. Parse rule_set and revision from URL
  | 2. Verify revision exists in rule_revisions
  | 3. Update rule_activations to point to new revision
  | 4. Workers fetch new revision on next cache miss
  |
  v
[Worker] --next event-->
  |
  | 1. Read activation for (tenant_scope, rule_set)
  | 2. Read revision from rule_revisions
  | 3. Cache revision in memory (invalidate on cache miss)
  | 4. Evaluate event against new revision
  |
  v
New events use new revision; existing executions use pinned revision
```

---

## 4. Execution Lookup Flow

```text
GET /v1/executions/{executionID}
```

```text
Client
  |
  v
[API Server]
  |
  | 1. Parse execution_id from URL
  |
  v
[ExecutionRepository.Get(execution_id)]
  |
  | SELECT execution_id, event_id, tenant_id, rule_set, revision,
  |        decision_hash, status, error, trace_id, created_at, completed_at
  | FROM executions
  | WHERE execution_id = $1
  |
  v
Return 200 OK with execution details
```

---

## 5. Replay Flow

```text
POST /v1/replays
Body: { event_id, tenant_id, rule_set }
```

```text
Client
  |
  v
[API Server]
  |
  | 1. Validate replay request
  | 2. Verify authorization (operator can replay)
  |
  v
[Replay Use Case]
  |
  | 1. Load original event from inbox
  | 2. Create new event envelope with new event_id
  |    - Same tenant_id, type, partition_key, data
  |    - New occurred_at = now
  | 3. Publish new event to JetStream
  | 4. Return replay event_id
  |
  v
[Worker] --processes like normal event-->
  |
  | New execution with new event_id
  | Same revision pinned (from original execution)
  | Same decision hash (deterministic)
  | New effect_ids (deterministic from new event_id + revision)
```

---

## 6. Quarantine Replay Flow

```text
POST /v1/quarantine/{id}/replay
```

```text
Client
  |
  v
[API Server]
  |
  | 1. Parse quarantine_id from URL
  |
  v
[QuarantineRepository.Get(quarantine_id)]
  |
  | SELECT quarantine_id, source_type, source_id, error_class, payload_ref, created_at
  | FROM quarantine
  | WHERE quarantine_id = $1
  |
  v
[QuarantineRepository.Replay(quarantine_id)]
  |
  | 1. Load quarantined event/execution
  | 2. Reset error state
  | 3. Create new event envelope
  | 4. Publish to JetStream
  | 5. Mark quarantine as replayed
  |
  v
[Worker] --processes like normal event-->
```

---

## 7. Effect Delivery Flow (Publisher)

```text
Publisher Loop --every 5s-->
```

```text
[Publisher]
  |
  | 1. ClaimPending(batch_size=10, owner="publisher")
  |    SELECT ... FROM outbox_effects
  |    WHERE status='pending' AND available_at <= NOW()
  |    ORDER BY available_at
  |    LIMIT 10
  |    FOR UPDATE SKIP LOCKED
  |
  v
For each claimed effect:
  |
  | 2. EffectSender.Send(effect)
  |    - Send to destination with effect_id as idempotency key
  |
  | On Success:
  |    3. MarkDelivered(effect_id)
  |       UPDATE outbox_effects SET status='delivered' WHERE effect_id=$1
  |
  | On Transient Failure:
  |    3. ScheduleRetry(effect_id, delay, attempts, error)
  |       - delay = exponential backoff (1s, 2s, 4s, 8s, 16s, 30s max)
  |       - attempts = attempts + 1
  |       - available_at = now + delay
  |       - If attempts >= max_attempts (5):
  |         Quarantine effect and create quarantine entry
  |
  | On Permanent Failure:
  |    3. Quarantine(effect_id, error)
  |       - Create quarantine entry
  |       - Mark effect as quarantined
```

---

## 8. Worker Lifecycle Flow

```text
Worker Start
  |
  | 1. Connect to PostgreSQL
  | 2. Run migrations
  | 3. Connect to NATS JetStream
  | 4. Create stream and consumer
  | 5. Start publisher loop (every 5s)
  | 6. Start fetch loop
  |
  v
[Fetch Loop]
  |
  | 1. Fetch(maxMessages=10)
  | 2. For each delivery:
  |    a. Deserialize event
  |    b. ProcessEventUseCase.Execute(event)
  |    c. On success: Ack(delivery)
  |    d. On permanent error: Nak(delivery)
  |    e. On transient error: Retry(delivery, 5s)
  | 3. Loop back to step 1
  |
  v
Worker Shutdown (SIGINT/SIGTERM)
  |
  | 1. Stop fetch loop
  | 2. Finish in-flight events
  | 3. Release shard leases
  | 4. Close NATS connection
  | 5. Close PostgreSQL connection
  | 6. Exit
```

---

## 9. Duplicate Detection Flow

```text
Event arrives (event_id=X)
  |
  v
[InboxRepository.Get(tenant_id, event_id)]
  |
  | SELECT ... FROM inbox WHERE tenant_id=$1 AND event_id=$2
  |
  v
If entry exists:
  |
  | Already processed or processing
  | Load execution from executions table
  | Return existing execution (no re-evaluation)
  |
If entry does not exist:
  |
  | Insert new inbox entry
  | INSERT INTO inbox (tenant_id, event_id, status='processing')
  | ON CONFLICT (tenant_id, event_id) DO NOTHING
  |
  | If insert succeeded:
  |   Process event normally
  |
  | If conflict (race condition):
  |   Load execution from executions table
  |   Return existing execution
```

---

## 10. Shard Lease Flow

```text
Worker starts
  |
  | 1. Compute virtual_shard for owned partitions
  | 2. Acquire lease with TTL (e.g., 30s)
  |
  v
[ShardLeaseRepository.Acquire(shard, owner, ttl)]
  |
  | INSERT INTO shard_leases (virtual_shard, owner, fencing_token, expires_at)
  | VALUES ($1, $2, 1, NOW() + ttl)
  | ON CONFLICT (virtual_shard) DO UPDATE SET
  |   owner = CASE WHEN expires_at < NOW() THEN EXCLUDED.owner ELSE owner END,
  |   fencing_token = CASE WHEN expires_at < NOW() THEN fencing_token + 1 ELSE fencing_token END,
  |   expires_at = CASE WHEN expires_at < NOW() THEN EXCLUDED.expires_at ELSE expires_at END
  |
  v
Worker processes events for owned shards
  |
  | Every 15s (half TTL): Renew lease
  | UPDATE shard_leases SET expires_at = NOW() + ttl
  | WHERE virtual_shard=$1 AND owner=$2 AND fencing_token=$3
  |
  | If renewal fails (fencing token mismatch):
  |   Another worker owns the shard
  |   Stop processing events for this shard
  |
  v
Worker stops
  |
  | DELETE FROM shard_leases WHERE virtual_shard=$1 AND owner=$2
```

---

## Summary Table

| Flow | Entry Point | Key Tables | Key Functions |
|---|---|---|---|
| Event Intake | `POST /v1/events` | inbox, executions, outbox_effects | `ProcessEventUseCase.Execute` |
| Rule Deployment | `POST /v1/rules/{ruleSet}/revisions` | rule_revisions, rule_activations | `ActivateRuleUseCase.Execute` |
| Rule Activation | `POST /v1/rules/{ruleSet}/revisions/{revision}/activate` | rule_activations | `ActivationRepository.Set` |
| Execution Lookup | `GET /v1/executions/{executionID}` | executions | `ExecutionRepository.Get` |
| Replay | `POST /v1/replays` | inbox, executions | `Replay UseCase` |
| Quarantine Replay | `POST /v1/quarantine/{id}/replay` | quarantine | `QuarantineRepository.Replay` |
| Effect Delivery | Publisher loop | outbox_effects | `PublishEffectsUseCase.Execute` |
| Worker Lifecycle | cmd/worker main | shard_leases | `Worker.Run` |
| Duplicate Detection | Inbox check | inbox | `InboxRepository.Insert` |
| Shard Lease | Worker startup | shard_leases | `ShardLeaseRepository.Acquire` |