# Feature Specification: <short-name>

> Keep this spec concise and testable. Mark non-applicable sections N/A with a reason; do not leave ambiguity hidden in blank fields.

## 1. Metadata

- **Status:** Proposed | Accepted | In progress | Implemented | Deprecated
- **Owner:**
- **Reviewers:**
- **Related issue/PR:**
- **Last updated:**

## 2. Problem and evidence

- **Problem:** What user/operator problem or production risk exists?
- **Evidence:** Reproduction, logs/metrics, user impact, or source location.
- **Current behavior:**
- **Desired outcome:**

## 3. Scope

### Goals
- 

### Non-goals
- 

### Affected surfaces
- Packages/files:
- API/event/schema/SDK:
- Storage/migrations:
- Configuration/deployment:
- Observability/operations:

## 4. Contract and trust boundaries

- **Caller / producer:**
- **Identity source and authorization:**
- **Tenant scope:**
- **Inputs and validation:**
- **Outputs/errors:**
- **Compatibility/versioning:**
- **External dependencies/destinations:**

## 5. Invariants and guarantees

List only guarantees required by this change. State their exact scope and how each is enforced.

| Invariant | Enforcement point | Proof/test |
| --- | --- | --- |
|  |  |  |

## 6. Acceptance criteria

Write observable Given/When/Then scenarios. Include success, failure, and recovery cases where relevant. Link each criterion to a test or mark it **Gap**.

- **AC-1:**
  - Given:
  - When:
  - Then:
  - Test / status:

## 7. Failure and recovery

Address applicable timeout/cancellation, dependency outage, duplicate delivery, uncertain commit, crash/restart, retry classification and exhaustion, stale ownership, quarantine/replay, and partial rollout.

## 8. Security and privacy

Cover authentication, authorization, tenant filtering, input limits, outbound destination/URL validation, secret handling, sensitive-data logging, and abuse cases.

## 9. Operational behavior

- **Logs/metrics/traces:**
- **Alerts and actionable thresholds:**
- **Backpressure/capacity limits:**
- **Health/readiness/shutdown:**
- **Runbook or recovery steps:**

## 10. Rollout and rollback

- **Migration/deployment order:**
- **Feature flags/configuration:**
- **Compatibility window:**
- **Rollback procedure and limits:**
- **Data repair/replay plan:**

## 11. Verification plan

| Acceptance criterion | Test layer and location | Command / CI check | Status |
| --- | --- | --- | --- |
|  |  |  | Not run |

Check only after actually completed:
- [ ] Unit/service tests
- [ ] Adapter/contract tests
- [ ] Live dependency integration/E2E tests, where required
- [ ] Race/failure-injection tests, where relevant
- [ ] Formatting, build, lint, security and schema/migration checks
- [ ] Exact PR-head CI reviewed

## 12. Risks, alternatives, open questions

- **Risks and mitigations:**
- **Alternatives considered and why rejected:**
- **Open questions / explicit gaps:**
