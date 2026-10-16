# Feature Specification: <name>

## Status
Proposed | Accepted | Implemented | Deprecated

## Problem
What problem or operational risk does this feature address?

## Goals
- 

## Non-goals
- 

## Actors and boundaries
- Tenant / caller:
- Service or worker:
- Persistence and messaging dependencies:

## Contract
Describe API/event inputs, outputs, schemas, compatibility, and validation.

## Invariants
List the properties that must remain true, including tenant boundaries, idempotency, ordering, transaction boundaries, and authorization as applicable.

## Behavior and acceptance criteria
Use observable, testable statements.

- Given ...
- When ...
- Then ...

## Failure and recovery
Cover timeouts, cancellation, duplicate delivery, process crash, uncertain commit outcome, dependency outage, retry exhaustion, and stale ownership as relevant.

## Security and privacy
Describe authentication, authorization, tenant scoping, input validation, secret handling, and sensitive-data logging constraints.

## Observability
Define useful logs, metrics, traces, and alertable failure signals.

## Rollout and compatibility
Describe migrations, event/API compatibility, feature flags, rollback, and operational rollout.

## Test plan
- [ ] Unit tests
- [ ] Component tests
- [ ] Integration tests with real dependencies where semantics matter
- [ ] Contract tests
- [ ] End-to-end tests where warranted

## Open questions
- 
