# FlowRule

A distributed rules engine that evaluates business events against configurable rule sets and produces durable effects.

## What it does

1. Accepts an event from a broker
2. Finds the active rule revision for that event type and tenant
3. Evaluates predicates, selects matching rules
4. Records the decision and outbox effects in one atomic transaction
5. An async publisher delivers effects to external destinations

## Docs

| File | Covers |
|------|--------|
| [architecture.md](architecture.md) | Project structure, ports, component boundaries, dependency rules |
| [design.md](design.md) | HLD, LLD, data model, transaction algorithm, consistency model |
| [rules.md](rules.md) | Rule shape, compilation, evaluation, operators, contracts |
| [operations.md](operations.md) | Deployment, failure scenarios, partitioning, scaling |
| [roadmap.md](roadmap.md) | Implementation status, gaps, target plan |

## Quick start

```bash
# prerequisites: postgres, nats
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/flowrule?sslmode=disable"
export NATS_URL="nats://localhost:4222"

go run ./cmd/api      # :8080
go run ./cmd/worker   # fetches from NATS
```

Deploy a rule:
```bash
curl -X POST localhost:8080/v1/rules/order.created/revisions \
  -d '{"rule_set":"order.created","revision":1,"mode":"first_match","rules":[{"id":"high-value","priority":100,"when":{"path":"$.total","op":"gte","value":1000},"then":[{"emit":{"topic":"orders.review","data":{"reason":"high_value"}}}]}]}'
```

## License

Proprietary.
