# BDD Scenarios for Critical FlowRule Behavior

These scenarios define desired observable behavior. They are not all implemented as executable tests. The status and current test locations are tracked in [the E2E test matrix](e2e-test-matrix.md). Do not mark a scenario complete merely because a similarly named mock-based test exists.

## Duplicate event delivery

**Given** a valid event has committed its inbox record, execution, and outbox records  
**When** JetStream delivers the same logical event again  
**Then** the worker creates no second logical execution  
**And** no duplicate logical outbox effects are created  
**And** the duplicate delivery is settled according to the implementation's documented policy

## Persistence failure before commit

**Given** a worker is processing a valid event  
**When** a repository operation fails before the transaction commits  
**Then** no partial inbox/execution/outbox state is visible  
**And** the message is not acknowledged as successfully processed  
**And** retry or quarantine behavior follows the configured classification and limits

## Crash after commit but before ACK

**Given** the processing transaction commits successfully  
**When** the worker stops before acknowledging the JetStream message  
**And** JetStream redelivers the event  
**Then** inbox deduplication prevents a second logical execution  
**And** the already-persisted outbox effects retain stable identities

## Tenant boundary

**Given** a request is authenticated and scoped to tenant A  
**When** it requests or mutates a resource owned by tenant B  
**Then** the resource is not disclosed and the mutation is rejected or has no effect  
**And** the test verifies storage-level tenant filtering, not only handler behavior  
**And** logs do not disclose tenant B's sensitive data

## Stale worker loses its lease

**Given** worker A holds a shard lease with fencing token N  
**And** ownership moves to worker B with a newer fencing token  
**When** worker A resumes and attempts to commit work using token N  
**Then** the storage boundary rejects the stale operation  
**And** worker B can progress without stale state being overwritten

## Per-key ordering during retry

**Given** events E1 and E2 share a tenant and partition key  
**And** E1 fails transiently  
**When** E2 becomes available  
**Then** behavior matches the explicitly documented ordering policy  
**And** E2 does not overtake E1 if strict per-key ordering is promised  
**And** retry exhaustion/quarantine is observable and recoverable

## Rule revision consistency

**Given** an execution selects immutable rule revision R1  
**When** revision R2 is published while processing is in progress  
**Then** the execution follows the defined revision-selection policy  
**And** cache refresh timing cannot silently change the revision used by that execution

## External effect delivery

**Given** an outbox effect is sent to a destination  
**When** the destination performs the effect but the response is lost  
**And** the outbox retries delivery  
**Then** the same stable effect ID/idempotency key is reused  
**And** duplicate suppression occurs only if the receiver implements that contract  
**And** the system does not claim exactly-once external execution without downstream guarantees

## Event arrives before its rule is activated

**Given** a valid event arrives before its rule is deployed  
**When** the worker initially cannot process it because no activation exists  
**And** the rule is later saved and activated  
**Then** redelivery eventually creates exactly one execution  
**And** the event is not prematurely quarantined or silently dropped

This scenario currently has executable E2E coverage in tests/e2e/event_before_rule_test.go. The remaining scenarios and skipped cases are listed in the test matrix.

## Test implementation rules

- Assert durable state and externally observable outcomes, not only mock call counts.
- Use PostgreSQL-backed tests for transaction rollback, uniqueness, and fencing semantics.
- Use NATS JetStream-backed tests for redelivery, ACK timing, and delivery exhaustion.
- Prefer barriers, explicit hooks, and bounded polling over arbitrary sleeps for race-sensitive assertions.
- Give every skipped test a reason and a follow-up acceptance criterion. Skips are visible debt, not passing evidence.
