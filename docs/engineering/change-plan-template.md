# Change Plan: <short-name>

> Use for multi-package, architectural, persistence, security, concurrency, or operationally risky changes. Keep it short; omit a section only with a stated reason.

## 1. Outcome

- **Problem / evidence:**
- **Measurable acceptance criterion:**
- **Non-goals:**

## 2. Current flow

Describe the relevant caller → service → adapter/dependency path and current failure behavior. Link source files and tests.

## 3. Proposed change

Describe the smallest viable design, data/control flow, and why it meets the acceptance criteria.

## 4. Scope and ownership

| File/package/surface | Responsibility of change | Owner/layer |
| --- | --- | --- |
|  |  |  |

Include affected contracts, migrations, configuration, SDKs, examples, CI, and docs.

## 5. Invariants and compatibility

- Tenant/security boundaries:
- Transaction/idempotency/order/fencing guarantees:
- API/event/schema/SDK compatibility:
- Dependency direction and existing abstractions to reuse:

## 6. Failure and operations

- Timeouts, cancellation, retries and exhaustion:
- Process restart, duplicate delivery and uncertain outcomes:
- Observability, alerts and runbook:
- Deployment order, rollback limits and data recovery:

## 7. Alternatives and trade-offs

| Option | Benefit | Cost/risk | Decision |
| --- | --- | --- | --- |
|  |  |  |  |

## 8. Acceptance-to-test map

| Criterion | Test type/location | Expected assertion | Status |
| --- | --- | --- | --- |
|  |  |  | Not run |

Tests must prove the guarantee at the correct boundary. Mark missing proof **Gap** rather than substituting mocks.

## 9. Ordered implementation steps

1. [ ] Add/update specification and acceptance criteria.
2. [ ] Add failing regression tests for the highest-risk behavior.
3. [ ] Implement the smallest cohesive change.
4. [ ] Update contracts, adapters, migrations, config, examples and docs as applicable.
5. [ ] Run focused checks, then all applicable CI-equivalent checks.
6. [ ] Review the complete diff and exact PR-head CI.

## 10. Exit criteria and residual risk

- [ ] Acceptance criteria satisfied by evidence.
- [ ] Compatibility/security/operational review complete.
- [ ] No unrelated churn or untracked generated changes.
- [ ] All required checks report actual outcomes.
- **Unverified behavior / remaining risk:**
- **Rollback constraints:**
