# Build and deployment story

This is the “start from zero” story for FlowRule.

## 1. Product goal

FlowRule is not a general workflow engine. It is a rules engine that executes deterministic business logic against durable events and emits effects reliably.

It must do four things well:

- accept input events
- choose the active rule revision
- evaluate rules against the event
- persist execution + effects durably

This document covers the deployment flow: the steps needed to stand up the system and make it live. It is intentionally separate from the runtime event flow described in [05-producer-consumer-and-requests.md](./05-producer-consumer-and-requests.md).

## 2. Build choices

The project intentionally keeps the runtime small and deliberate:

- Go as the main runtime language
- Postgres as the transactional source of truth
- NATS JetStream as the durable broker
- stateless worker processes for evaluation
- immutable compiled rule revisions
- outbox-based effect publishing

This is the design implemented in:

- [cmd/api/main.go](../../cmd/api/main.go)
- [cmd/worker/main.go](../../cmd/worker/main.go)
- [internal/adapters/nats/nats.go](../../internal/adapters/nats/nats.go)
- [internal/adapters/sql](../../internal/adapters/sql)

## 3. Deployment topology

A minimal deployment is:

```text
[Clients]
    |
    v
[API process]
    |
    +--> Postgres (rule control plane)

[Event producers]
    |
    +--> NATS JetStream
    |
    v
[Worker process]
    |
    +--> Postgres
    +--> NATS JetStream
    |
    v
[Outbox publisher]
    |
    +--> destinations / effect topics
```

The API handles rule lifecycle operations and execution lookup. Producers publish events directly to NATS; there is no event-ingest HTTP endpoint in the current code. The worker handles event consumption and evaluation. The effect publisher reads pending outbox rows and forwards them to destinations.

## 4. Deployment build from scratch

The deployment flow has five stages, and each stage is a prerequisite for the next one:

```text
infra exists
  -> schema exists
  -> API process starts
  -> worker process starts
  -> rule is deployed and activated
```

### Step 1: prepare infrastructure

You need:

- PostgreSQL instance
- NATS server with JetStream enabled
- app environment variables

Typical env variables are:

```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/flowrule?sslmode=disable"
export NATS_URL="nats://localhost:4222"
export MIGRATIONS_DIR="migrations"
```

The SQL layer bootstraps the schema automatically through the migrations runner in the database startup path.

Important production distinction: migrations should be treated as a release-time deployment step, not as something the worker re-runs on every startup. Running migration logic on every runtime process is a convenience for local dev, not a production control-plane guarantee.

### Step 2: run database migrations

The database is initialized as a release-time deployment step. The migration set is under:

- [migrations](../../migrations)

This includes rule revision, activation, inbox, execution, outbox, shard lease, and quarantine tables.

The important production principle is: deployment owns schema creation; runtime owns event processing.

### Step 3: start the API

```bash
go run ./cmd/api
```

The API exposes rule deployment and execution querying endpoints. The build here is a slim composition root, not a domain service layer.

### Step 4: start the worker

```bash
go run ./cmd/worker
```

The worker connects to NATS JetStream, creates a durable consumer (target: `MaxDeliver` limit of 10, current: not yet configured), fetches messages, evaluates them, and writes the execution result to SQL. It acknowledges only after the SQL transaction commits; that ordering is the core runtime correctness rule.

### Step 5: publish a rule

The API accepts a JSON rule revision and activates it. A rule is validated and stored as an immutable source revision with a hash and compiled artifact.

### Step 6: ingest an event

The event is validated and published to the durable subject. The worker consumes it, pins the active revision, evaluates it, writes the execution, and stores outbox effects.

### Step 7: deliver effects

The publisher loop claims pending outbox rows and sends them to destinations. The destination payload is keyed by a deterministic effect identity.

## 5. Deployment expectations

A production-like deployment should have:

- HA Postgres with backups
- durable JetStream storage
- retry and quarantine handling
- tenancy isolation
- rate limits and quotas
- monitoring for lag, execution failures, and outbox growth

This should also include health checks that are not process-only: a readiness check should confirm Postgres and JetStream are reachable, not just that the process stayed alive.

## 6. Why this is a clean deployment model

This setup preserves a clear separation of concerns:

- API is for control plane and intake
- worker is for evaluation and state transitions
- Postgres is the durable truth
- broker is the reliable transport layer
- effect publisher is the final delivery step

That makes deployment simpler than a monolithic workflow engine and easier to scale horizontally by partition key.
