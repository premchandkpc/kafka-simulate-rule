# Story 01 — Build and local bootstrap

## Goal

Start the rule-management API and event worker from a clean checkout. The worker requires PostgreSQL and a NATS server with JetStream enabled; FlowRule does not provision either dependency.

## Prerequisites

- Go **1.26** (declared by `go.mod`)
- PostgreSQL reachable through `DATABASE_URL`
- NATS reachable through `NATS_URL`, running JetStream
- Network access to fetch Go modules on the first build

The default local settings are:

| Setting | Default |
| --- | --- |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/flowrule?sslmode=disable` |
| `NATS_URL` | `nats://localhost:4222` |
| `MIGRATIONS_DIR` | `migrations` |
| API listener | `:8080` |
| Stream / consumer / subject filter | `flowrule` / `flowrule-worker` / `events.>` |

Create the `flowrule` database and start NATS with JetStream using your organization’s normal local tooling. There is intentionally no Compose or Helm file in this repository today.

## Build and verify

From the repository root:

```sh
go test ./...
go build -o bin/api ./cmd/api
go build -o bin/worker ./cmd/worker
```

Run each process in a separate terminal:

```sh
DATABASE_URL='postgres://…' MIGRATIONS_DIR=migrations ./bin/api
DATABASE_URL='postgres://…' NATS_URL='nats://…' MIGRATIONS_DIR=migrations ./bin/worker
```

Both binaries run every `migrations/*.sql` file at startup. They are written with `CREATE … IF NOT EXISTS`, so repeat startup is normally harmless. This is convenient locally but is not a controlled production migration practice; run migrations once as a release job before rolling workloads.

The API health check is `GET /health`, returning `{"status":"ok"}`. A healthy API only proves the HTTP process is alive; it does not check database connectivity after startup or NATS availability.

## First rule and event

Deploying a revision also makes it active in the current API. The HTTP API always uses tenant scope `default`.

```sh
curl -X POST http://localhost:8080/v1/rules/order-created/revisions \
  -H 'content-type: application/json' \
  --data '{
    "rule_set":"order-created", "revision":1, "mode":"first_match",
    "rules":[{
      "id":"high-value", "priority":100,
      "when":{"path":"$.total","op":"gte","value":1000},
      "then":[{"emit":{"topic":"orders.review-requested","data":{"reason":"high_value"}}}]
    }]
  }'
```

Publish this envelope to a subject matching `events.>`—for example `events.order-created`—using a NATS client:

```json
{
  "id":"evt_001",
  "type":"order-created",
  "tenant_id":"default",
  "partition_key":"order-123",
  "occurred_at":"2026-09-22T10:00:00Z",
  "data":{"total":1200}
}
```

The `type` must equal the deployed rule set. The worker looks up activation using `(envelope.tenant_id, envelope.type)`; an event for another tenant will not find the API-created `default` activation.

## Build-to-production gap

Before calling this deployable, add immutable container builds, a migration job, configuration/secret injection, TLS, readiness/liveness checks, resource limits, NATS/Postgres HA, destination adapters, and monitoring. Story 02 gives the target deployment; Story 05 lists the correctness work that must precede production traffic.
