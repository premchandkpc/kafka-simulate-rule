# Story 03 — Request and event journeys

## Supported request/message types

| Initiator | Type | Route/subject | Result today |
| --- | --- | --- | --- |
| Rule administrator | Deploy revision | `POST /v1/rules/{ruleSet}/revisions` | Compiles, saves, and activates it for scope `default` |
| Rule administrator | Activate an existing revision | `POST /v1/rules/{ruleSet}/revisions/{revision}/activate` | Returns 501; not implemented |
| Operator | Get execution | `GET /v1/executions/{executionID}` | Returns the stored execution or 404 |
| Platform | Health | `GET /health` | Process-only health status |
| Event producer | Business event | NATS subject matching `events.>` | Worker evaluates it after JetStream delivery |
| Outbox publisher | Effect delivery | Adapter-specific | Fake destination records delivery only |

There is no ingress endpoint for events, no authentication/authorization, no rule-list/read endpoint, no replay API, and no real command/event destination protocol in this codebase.

## Rule deployment journey

```text
POST rule JSON
  -> compiler validates and sorts rules by descending priority
  -> rule_revisions insert (conflict ignored)
  -> rule_activations upsert
  -> activation JSON response
```

The supplied `{ruleSet}` path value overwrites the compiled rule set name. The caller should keep the URL and JSON `rule_set` identical to avoid storing content whose source metadata disagrees with the registry key. Revision uniqueness is `(tenant_scope, rule_id, revision)`.

## Event-to-effect journey

```text
producer publishes EventEnvelope to events.<name>
  -> JetStream durable pull consumer fetches up to 10
  -> worker parses and validates envelope
  -> BEGIN TRANSACTION
     -> inbox duplicate lookup/insert
     -> activation lookup using (tenant_id, event.type)
     -> active rule revision lookup
     -> pure evaluator creates decision + deterministic effects
     -> execution and outbox rows written
     -> inbox marked committed
  -> COMMIT
  -> broker ACK (only after commit)
  -> five-second publisher loop claims pending effects
  -> destination Send
  -> delivered, retry, or quarantine state
```

For an invalid message that cannot be decoded as an envelope, the worker calls `Nak`; for a processing error it calls `Retry(5s)` unless `domain.IsPermanent(err)` says otherwise. Because JetStream `MaxDeliver` is left at its library zero value in the worker configuration, set and verify an explicit poison-message policy before production.

## Duplicate and recovery journey

The inbox key is `(tenant_id, event_id)`. A duplicate whose row points to an existing execution returns that execution and is ACKed without reevaluating rules. Effect IDs are deterministic:

```text
SHA-256(tenant_id | event_id | rule_set | revision | rule_id | action_index)
```

That allows a correctly implemented destination to deduplicate redelivery. The local writes are now inside a single Postgres transaction; a crash at any point rolls back cleanly and redelivery finds the state it expects (see Story 05, gap 1 — fixed).

## Effect retry journey

The publisher claims pending rows and sends each effect. On failure it sets attempts and schedules delays of 1, 2, 4, 8, then 16 seconds (up to 60 seconds); at the fifth failed attempt it marks the outbox row quarantined. The in-memory quarantine repository used by the worker loses the accompanying quarantine entry on restart even though the SQL table exists.
