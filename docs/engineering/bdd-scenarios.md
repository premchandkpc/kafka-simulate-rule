# BDD Scenarios for Critical FlowRulZ Behavior

These scenarios are acceptance examples. Adapt names and fixtures to the existing implementation. They are not executable tests until connected to the project's test harness.

## Duplicate event delivery

**Given** a valid event has already committed its inbox record, execution, and outbox records  
**When** the same logical event is delivered again  
**Then** the worker does not create a second logical execution  
**And** it does not create duplicate logical outbox effects  
**And** the message can be acknowledged according to the documented duplicate-delivery policy

## Failure before transaction commit

**Given** a worker is processing a valid event  
**When** persistence fails before the transaction commits  
**Then** the transaction leaves no partial inbox/execution/outbox state  
**And** the message is not acknowledged as successfully processed  
**And** retry behavior follows the configured policy

## Crash after commit but before acknowledgement

**Given** the processing transaction has committed successfully  
**When** the worker crashes before acknowledging the message  
**And** JetStream redelivers the event  
**Then** inbox deduplication prevents a second logical execution  
**And** already-persisted outbox effects retain their stable identities

## Tenant boundary

**Given** an authenticated caller belongs to tenant A  
**When** the caller requests a resource belonging to tenant B  
**Then** the request is denied or the resource is not exposed  
**And** the operation does not mutate tenant B's data  
**And** logs do not disclose tenant B's sensitive data

## Stale worker loses its lease

**Given** worker A holds a lease with fencing token N  
**And** worker A pauses long enough for ownership to move to worker B with a newer token  
**When** worker A resumes and attempts to commit stale work  
**Then** the storage boundary rejects the stale commit  
**And** worker B can make progress without stale state being overwritten

## Per-key ordering during retry

**Given** events E1 and E2 share the same ordering key  
**And** E1 fails transiently  
**When** E2 is available for processing  
**Then** the behavior follows the documented per-key retry policy  
**And** E2 does not overtake E1 when strict per-key ordering is promised  
**And** terminal failure or quarantine behavior is observable and recoverable

## Rule revision consistency

**Given** an execution is bound to rule revision R1  
**When** a newer revision R2 is published while the execution is in progress  
**Then** the execution follows the documented revision-selection policy  
**And** cache refresh timing does not silently change the revision used mid-execution

## External effect delivery

**Given** an outbox effect is delivered to an external system  
**When** the acknowledgement is lost and delivery is retried  
**Then** the same stable effect identity is reused  
**And** duplicate suppression is applied by the downstream system where supported  
**And** the system does not claim exactly-once external execution unless the downstream contract actually guarantees it

## Test implementation guidance

- Use unit tests for deterministic rule and retry classification logic.
- Use PostgreSQL integration tests for uniqueness, transaction rollback, and lease fencing.
- Use NATS JetStream integration tests for redelivery, acknowledgement timing, and retry exhaustion.
- Assert persisted state and observable delivery outcomes, not only mock call counts.
- Make concurrency tests deterministic with barriers/channels rather than arbitrary sleeps where possible.
