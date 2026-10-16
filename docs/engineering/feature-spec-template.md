# Feature Specification: <name>

## Status and ownership
- Status: Proposed | Accepted | Implemented | Deprecated
- Owner:
- Reviewers:
- Last updated:

## Problem
What user/operator problem or production risk does this address? Include evidence or a reproducible example.

## Goals
- 

## Non-goals
- 

## Actors and boundaries
- Caller and trusted identity source:
- Tenant scope:
- API, worker, or background process:
- Persistence and messaging dependencies:
- External destinations:

## Contract and compatibility
Describe request/event inputs, outputs, schema/version, validation, defaults, size limits, and compatibility with existing producers/consumers.

## Invariants
List the properties that must remain true, such as tenant isolation, idempotency, ordering, transaction boundaries, lease fencing, authorization, and data retention.

## Behavior and acceptance criteria
Write observable Given/When/Then examples. Include at least one failure or recovery path when the change touches distributed state.

- Given ...
- When ...
- Then ...

## Failure and recovery
Cover applicable timeouts, cancellation, duplicate delivery, uncertain commit outcome, process crash, dependency outage, retry classification/exhaustion, stale ownership, quarantine, and replay.

## Security and privacy
Describe authentication, authorization, tenant scoping, input validation, outbound URL/destination validation, secret handling, and sensitive-data logging constraints.

## Observability
Define useful structured logs, metrics, traces, alertable signals, and correlation/idempotency IDs.

## Rollout and compatibility
Describe migration ordering, backfill, API/event compatibility, configuration/feature flags, rollout, rollback limits, and recovery procedure.

## Test plan
Map each acceptance criterion to a test layer and exact test location/command. Mark gaps honestly.

- [ ] Unit tests for pure decisions
- [ ] Service/component tests for orchestration
- [ ] Adapter/contract tests for storage and transport boundaries
- [ ] Integration tests, noting whether dependencies are real or mocked
- [ ] E2E tests for critical paths where warranted
- [ ] Negative/security tests for tenant boundaries where relevant
- [ ] Race/failure-injection tests for concurrency and crash-sensitive behavior
- [ ] Required CI gates run against the final commit

## Open questions and known gaps
- 
