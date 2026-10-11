# Follow-up repository review — 2026-10-11

## Scope and baseline

This follow-up review started from main commit `b25fcd93795b35ca40df244f3c9bcc64c52c024d`, after verifying that the prior main CI run was green and the accidental unreviewed test-file commits had been reverted. The review traced HTTP handler to service to adapter paths, SQL migrations, scheduled-event lifecycle, batch runtime, outbox claim/settlement, Mongo document serialization, output path checks, the six Go module boundaries, and current CI evidence.

The source updates are in draft PR #5, branch `fix/full-review-scheduled-events-and-output-path`. The complete Go CI passed again at source commit `31e4d3a8a90f98ea48bd209f407744c794c3ba80` in [run 38120568255](https://github.com/premchandkpc/kafka-simulate-rule/actions/runs/38120568255). It passed formatting, JSON schema and shell script validation, root build, race-enabled root tests, vet, both SDK modules, and all three example modules. Later commits add documentation and the current branch tip therefore still needs a fresh Go CI and CodeQL check before merge. No local test run is claimed.

## Confirmed defects fixed in the follow-up branch

### 1. Scheduled-event schema and claim lifecycle

- SQL scheduled-event code referenced `claimant`, `claimed_at`, `claim_expires_at`, and `last_error` columns that were absent from migration 011. Added forward-only migration 015 rather than editing an already-applied migration.
- SQL and Mongo now include expired claimed events in the due set so they can be reclaimed after a worker crash.
- Claims return to the scheduler with a unique per-tick claimant token. Release/failure settlement requires that token and an unexpired claim.
- Scheduled-event list/get/cancel operations are tenant-scoped in repository methods; list pagination is validated/capped. Cancellation only succeeds for a still-pending event and returns a conflict otherwise.
- Added scheduled-handler and scheduler claim-token regression tests.
- Remaining: a broker publish failure is currently marked terminally failed rather than retried with bounded backoff.

### 2. Generation output path containment

- Replaced an unsafe string-prefix check with path-relative containment plus symlink resolution for existing path components.
- Rejects sibling prefixes such as `/tmp/flowrule-generated-evil`, parent traversal, symlink escapes in existing child paths, and a symlink used as the output root itself.
- The dedicated root directory is forced to mode `0700` before generation.
- Added regression tests for valid nested paths, traversal, sibling prefixes, child symlinks, and symlink-root rejection.

### 3. Outbox claimant fencing and durable error handling

- Outbox delivery, retry and quarantine settlement now require the unique claim token and a claim that has not expired, in both SQL and Mongo.
- The effect publisher uses a unique per-publish claim token and preserves the outbox effect ID as the external idempotency key.
- Settlement and retry failures are returned/aggregated rather than silently logged.
- Quarantine records use claim-specific IDs and idempotent persistence; after a lost claim, cleanup targets only the record created by that claim.
- HTTP effect delivery now rejects all non-2xx statuses, avoids copying downstream response bodies into errors, and no longer logs the full endpoint URL where credentials/query parameters could be present.
- Remaining: quarantine record persistence and outbox status transition are separate operations. A cleanup failure can still leave an orphan record; a live failure-injection test and a repairable/atomic protocol are still required. Delivery remains sequential under a fixed two-minute claim TTL, without per-effect timeouts.

### 4. Mongo JSON representation

Several adapters treated JSON byte sequences as `bson.Raw`, even though that type represents BSON-encoded document bytes. Those raw JSON bytes are now represented as BSON binary byte slices, and headers are stored as normal string maps. Rule source and compiled rules are preserved as JSON bytes. Added BSON round-trip tests for event data/headers, inbox/outbox payloads, scheduled events, workflow context, and rule documents.

This prevents the JSON/BSON representation mismatch, but does not establish full Mongo parity: transactions still require a transaction-capable Mongo topology and the adapter contract suite is not yet run against a live Mongo instance.

### 5. Batch queries and fail-closed behavior

- Implemented tenant-scoped batch retrieval/listing in SQL and Mongo, including bounded pagination and an optional rule-set filter.
- HTTP GetBatch now passes the tenant identity to the service; body tenant mismatches are rejected on workflow, batch and scheduled-event creation.
- Fixed a Mongo batch-run upsert that tried to update the same fields under both `$set` and `$setOnInsert`.
- Batch creation now returns a typed unavailable error instead of dereferencing a missing processor; the HTTP API maps this to 503.
- Config now rejects non-`none` `BATCH_MODE` values. This is intentional fail-closed behavior, not a claim that batch processing is supported: the current batch service invokes event processing with a zero fencing token and bypasses per-key queue integration. Do not remove this guard until a real lease/fencing and key-queue design exists.

### 6. Schema and documentation accuracy

- Updated rule schema patterns to accept rule-set names and rule identifiers used by checked-in examples.
- Replaced stale operations text about worker settings and stream subjects with actual runtime behavior.
- Corrected an inaccurate block in `AGENTS.md` that described a trusted-proxy token mode. That code is not present in `TenantExtractor`; no token, JWT or mTLS authentication is implemented.
- Updated README, architecture map, operations guide and this audit to label unsupported batch execution and current shard ownership assumptions.

## Open findings and priority

### P0 — No authenticated tenant identity or authorization

The current `TenantExtractor` trusts the caller-controlled `X-Tenant-ID` header, then the `tenant_id` query parameter, then defaults to `default`. There is no trusted-proxy token validation, JWT/mTLS authentication, or general authorization middleware. Body/header consistency checks do not establish identity.

Some operations are now tenant-filtered, but resource-level scoping is not yet complete across all workflow and quarantine paths. Keep the API private/local until trusted identity is introduced, every resource lookup/mutation authorizes the tenant, and cross-tenant negative tests exist.

### P1 — Shard assignment assumes a three-worker topology

`LeaseManager.Start` and rebalancing target `NUM_SHARDS / 3` per worker, without configured membership or a rebalance protocol. A deployment with a different replica count may leave shards unowned. Documenting it is not a fix: add membership/rebalancing or a validated, explicit fixed-replica contract, then test that all shards are assigned during startup, worker join/leave, and lease expiry.

### P1 — Batch execution is not supported

The event processor rejects a zero fencing token, while `batches.Service.processGroup` currently passes zero and bypasses the key queue. Worker config now rejects non-none batch modes, and API batch creation fails safely when no processor is configured. Keep this feature disabled until it can get a real lease/fencing token and preserve per-key order.

### P1 — Outbox recovery still needs live failure testing

Claim tokens and unexpired-claim checks now fence SQL/Mongo settlement, but the remote request may succeed before the worker crashes or loses the claim. Receiver-side idempotency remains essential. Run live PostgreSQL/NATS tests for claim expiry, concurrent publishers, ACK/settlement loss, destination timeout, database outage during quarantine, and cleanup failure.

### P1 — No live adapter conformance suite

The existing contract and integration suites rely on in-memory repositories/mocks. They do not prove the same transaction, uniqueness, rollback, redelivery, Mongo serialization, or lease-fencing semantics against real PostgreSQL and Mongo.

### P2 — Scheduled-event failure policy

A failed scheduled publish is marked failed immediately rather than classified/retried with backoff. Define retryable and permanent errors, bounded attempts, and recovery tests.

### P2 — Remaining architectural debt

- `ports.Transaction` exposes transaction context/fencing internals and `TxRepos` aggregates unrelated repositories.
- `internal/factory` overlaps the active `internal/storage` composition path.
- Workflow service definition methods still contain not-implemented placeholders while HTTP handlers use a separate definition repository.
- `/health` is liveness-only; gRPC is not implemented; migration CLI supports `up` only.
- SQL outbox insert is row-by-row; no reproducible throughput/p99 benchmarks or retention review exists.

## Recommended order

1. Trusted authN/authZ and complete tenant scoping.
2. Shard membership/rebalancing and explicit batch processing design (keep batch disabled until then).
3. Live PostgreSQL/NATS and Mongo contract/failure-injection tests.
4. Transaction-port narrowing and removal of duplicate composition.
5. Scheduled-event retry policy, readiness, retention, and performance measurement.

## Evidence rule

Re-run and record all Go CI and CodeQL jobs against the exact PR head immediately before merge. A green compile/test does not close the security or live-infrastructure risks listed above.
