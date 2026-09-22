# Example 2 — Failure scenarios

## 1. Crash BEFORE commit

```text
Worker
   |
   v
BEGIN
   |
   v
INSERT inbox
   |
   v
INSERT execution
   |
   v
   X  CRASH
```

Result:

```text
inbox       -> rolled back
execution   -> rolled back
outbox      -> rolled back
broker ACK  -> never sent
```

NATS redelivers. Worker retries safely.

```text
No commit + No ACK = Safe retry
```

## 2. Crash AFTER commit but BEFORE ACK

```text
Worker
   |
   v
BEGIN
   |
   v
INSERT inbox, execution, outbox
   |
   v
COMMIT  ✓
   |
   v
   X  CRASH (before delivery.Ack())
```

Result:

```text
inbox       -> committed (status=committed)
execution   -> committed
outbox      -> committed
broker ACK  -> not sent
```

NATS redelivers the same event.

```text
Second delivery:
   |
   v
Inbox.Get(tenant_id, event_id)
   -> entry exists, status=committed
   |
   v
Inbox.Insert(tenant_id, event_id)
   -> false (duplicate)
   |
   v
Return existing execution (idempotent)
   |
   v
delivery.Ack()
```

```text
Broker deliveries = 2
Executions = 1
Effects = 1
```

This is the inbox doing its job.

## 3. Destination is DOWN

```text
Outbox Publisher
   |
   v
ClaimPending -> effect claimed
   |
   v
EffectSender.Send(effect)
   |
   v
fraud-service = DOWN
   |
   v
HTTP timeout
   |
   v
ScheduleRetry(effect_id, delay=1s, attempts=1)
   |
   v
... later ...
   |
   v
ClaimPending -> effect claimed again
   |
   v
EffectSender.Send(effect)
   |
   v
fraud-service = DOWN
   |
   v
ScheduleRetry(effect_id, delay=2s, attempts=2)
   |
   v
... max attempts reached ...
   |
   v
Quarantine(effect_id)
```

The event itself was already processed. Only the effect delivery failed.

```text
Decisioning failure  ≠  Effect delivery failure
```

## 4. Poison message (invalid JSON)

```text
Worker
   |
   v
delivery.Event() -> nil (JSON parse failed)
   |
   v
Quarantine(invalid event)
   |
   v
delivery.Ack()
```

The event is removed from the retry loop. It does not block other events.

## 5. Two workers process the same event

```text
Worker A                    Worker B
   |                           |
   v                           v
BEGIN                       BEGIN
   |                           |
   v                           v
Inbox.Get -> nil             Inbox.Get -> nil
   |                           |
   v                           v
Inbox.Insert -> true         Inbox.Insert -> false (conflict)
   |                           |
   v                           v
... process ...              Inbox.Get -> entry
   |                           |
   v                           v
COMMIT                       return existing execution
   |                           |
   v                           v
ACK                          ACK
```

Both workers complete. Only one execution is created. The UNIQUE constraint on `(tenant_id, event_id)` prevents duplicates.

## 6. Worker crash during outbox publish

```text
Outbox Publisher
   |
   v
ClaimPending -> 5 effects
   |
   v
Send(effect-1) -> OK
   |
   v
MarkDelivered(effect-1) -> OK
   |
   v
Send(effect-2) -> OK
   |
   v
   X  CRASH
```

Result:

```text
effect-1 -> delivered ✓
effect-2 -> claimed (stale claim)
effect-3 -> pending
effect-4 -> pending
effect-5 -> pending
```

After restart:

```text
ClaimPending
   |
   +-- effect-2: claimed AND claim_expires_at <= NOW()
   |   -> reclaim it
   |
   +-- effect-3,4,5: pending
   |   -> claim them
```

The `claim_expires_at` column (1 minute TTL) recovers stale claims.

## Current implementation status

| Scenario | Status |
|----------|--------|
| Crash before commit | ✅ safe (transaction rollback) |
| Crash after commit, before ACK | ✅ safe (inbox dedup) |
| Destination down | ✅ safe (outbox retry + quarantine) |
| Poison message | ✅ safe (quarantine + ack) |
| Two workers, same event | ✅ safe (unique constraint) |
| Outbox publisher crash | ✅ safe (claim_expires_at recovery) |
