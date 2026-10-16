# BDD Acceptance Scenarios: FlowRule

These scenarios define expected outcomes; they are not all implemented as automated tests. Current evidence and gaps are tracked in the [E2E test matrix](e2e-test-matrix.md).

## Scenario format

Each scenario MUST describe an observable outcome. Link it to an automated test or mark it **Gap**. Do not count mocks as proof of database, broker, or process-failure behavior.

## Duplicate event delivery

**Given** an event's inbox, execution, and outbox records have committed  
**When** JetStream redelivers the same logical event  
**Then** no second logical execution or duplicate logical outbox record is created  
**And** delivery settlement follows the documented policy

## Persistence failure before commit

**Given** a worker is processing a valid event  
**When** a write fails before the transaction commits  
**Then** no partial inbox/execution/outbox state is visible  
**And** the message is not acknowledged as successfully processed  
**And** retry or quarantine follows the configured policy

## Crash after commit, before ACK

**Given** the processing transaction commits  
**When** the worker stops before ACK and JetStream redelivers the event  
**Then** deduplication prevents a second logical execution  
**And** persisted outbox effects retain stable identities

## Tenant boundary

**Given** a request is authenticated as tenant A  
**When** it reads or mutates a resource owned by tenant B  
**Then** tenant B's data is not disclosed and the mutation has no effect  
**And** the test verifies storage-level tenant scoping, not only handler behavior

## Stale lease holder

**Given** worker A owns a shard with fencing token N  
**And** worker B acquires ownership with a newer token  
**When** worker A resumes and attempts to commit with token N  
**Then** the storage boundary rejects the stale operation  
**And** worker A cannot overwrite worker B's state

## Per-key ordering during retry

**Given** E1 and E2 share a tenant and partition key  
**And** E1 fails transiently  
**When** E2 becomes eligible for processing  
**Then** observed order matches the documented guarantee  
**And** if strict per-key ordering is promised, E2 does not overtake E1  
**And** retry exhaustion or quarantine is observable and recoverable

## Rule revision consistency

**Given** an execution selects immutable revision R1  
**When** revision R2 is activated while processing is in progress  
**Then** the execution follows the documented revision-selection policy  
**And** cache refresh timing does not silently change the selected revision

## External effect delivery

**Given** a destination performs an effect but its response is lost  
**When** the outbox retries delivery  
**Then** the same stable effect ID/idempotency key is reused  
**And** duplicate suppression is claimed only if the receiver implements it  
**And** the system does not claim exactly-once external execution without downstream guarantees

## Event arrives before rule activation

**Given** a valid event arrives before its rule is active  
**When** the rule is subsequently saved and activated  
**Then** redelivery eventually creates exactly one execution  
**And** the event is not prematurely quarantined or silently dropped

**Current evidence:** this scenario has E2E coverage in tests/e2e/event_before_rule_test.go. See the [test matrix](e2e-test-matrix.md) for other scenarios and gaps.

## Test quality rules

- Assert durable state and externally observable outcomes, not only mock call counts.
- Use PostgreSQL-backed tests for transaction, uniqueness, and fencing semantics.
- Use NATS JetStream-backed tests for redelivery, ACK timing, and delivery exhaustion.
- Use explicit barriers/hooks or bounded state polling for race-sensitive assertions.
- Every skipped test MUST state why it is skipped and what acceptance criterion will replace the skip.
