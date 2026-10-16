# SDD, TDD, and BDD for FlowRulZ

This guide defines how feature work should move from intent to verified behavior in FlowRulZ. It is a lightweight engineering workflow, not a requirement to create paperwork for every change.

## The three practices

- **Spec-Driven Development (SDD):** agree on the intended behavior, constraints, interfaces, and failure semantics before implementation.
- **Test-Driven Development (TDD):** write a focused failing test, implement the smallest correct change, then refactor while keeping tests green.
- **Behavior-Driven Development (BDD):** describe externally observable behavior as scenarios using Given/When/Then language that product, engineering, and operations can review.

They complement each other: SDD defines what must be true, BDD makes important behavior examples explicit, and TDD drives implementation against executable tests.

## Recommended workflow

1. **Clarify the problem.** Document the user/operator impact, scope, non-goals, and assumptions.
2. **Write or update the spec.** Include API/event contracts, invariants, authorization boundaries, failure behavior, observability, and acceptance criteria.
3. **Add BDD scenarios.** Cover the happy path and the most important negative and recovery paths. Scenarios may be Markdown examples; use an executable BDD framework only when it adds value.
4. **Drive implementation with TDD.** Add unit tests first for domain/application logic, then implement and refactor.
5. **Verify integration boundaries.** For database transactions, NATS JetStream delivery, leases, and outbox processing, use integration tests with real dependencies or faithful containers rather than relying only on mocks.
6. **Run quality gates.** Run formatting, unit tests, integration tests, lint, security scanning, and build checks applicable to the changed code.
7. **Review and reconcile.** Ensure code, tests, docs, schemas, and operational behavior agree. Update the spec when requirements intentionally change.

## FlowRulZ correctness invariants

Feature specifications and tests should explicitly address applicable invariants:

- **Tenant isolation:** every tenant-scoped operation is authorized and scoped at the storage boundary.
- **Idempotent processing:** duplicate delivery does not create duplicate logical executions or duplicate logical effects.
- **Transaction boundaries:** inbox acceptance, execution state, and outbox records are committed atomically when required by the design.
- **Acknowledgement ordering:** acknowledge a message only after the durable processing outcome is committed; document any deliberate exceptions.
- **Per-key ordering:** retries and terminal failures must follow an explicit policy and must not silently violate the promised ordering guarantee.
- **Lease fencing:** stale workers cannot commit after ownership has moved to a newer fencing token.
- **Rule revision consistency:** one execution uses a well-defined rule revision, independent of incidental cache state.
- **External effects:** do not claim exactly-once execution of arbitrary external side effects. Use stable effect IDs and downstream idempotency/deduplication where supported.

These are design targets to validate against the current implementation, not claims that every invariant is already satisfied.

## Test layers

| Layer | Best for | Examples |
| --- | --- | --- |
| Unit | Pure logic and local decisions | Rule evaluation, validation, retry classification |
| Component | One service/component with controlled dependencies | Handler behavior, adapter mapping, configuration |
| Integration | Real storage and messaging semantics | PostgreSQL transaction rollback, unique constraints, JetStream redelivery |
| Contract | Compatibility across producers/consumers | Event envelope versions, API schemas |
| End-to-end | Critical user/operator journeys | Submit event, execute rule, persist run, deliver effect |

Prefer fast unit tests for breadth and a smaller set of integration tests for the failure modes mocks cannot prove.

## Definition of Done

- [ ] Spec and acceptance criteria reviewed or updated.
- [ ] Happy path, validation failures, authorization failures, and relevant recovery paths covered.
- [ ] Tests were written before implementation where practical; bug fixes include a regression test.
- [ ] Integration tests cover changed transaction/message/lease boundaries.
- [ ] Formatting, tests, lint, build, and applicable security checks pass.
- [ ] Logs, metrics, and error handling make failures diagnosable without leaking secrets or tenant data.
- [ ] Compatibility and migration implications are documented.
- [ ] PR description states guarantees, trade-offs, and known limitations.

## Keep it proportional

For a typo or isolated refactor with no behavior change, a short test and review may be enough. For changes to idempotency, retries, tenant isolation, lease fencing, event contracts, or persistence, use the full workflow. The goal is fewer ambiguous requirements and regressions, not more documents.
