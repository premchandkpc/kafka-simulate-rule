# Rust worker runtime (experimental)

## Goal

Provide a separately deployable Rust implementation alongside the existing Go worker. Go remains the default and its source is not removed or rewritten.

## Runtime selection

The production selector must be an explicit, validated `WORKER_RUNTIME=go|rust` setting. Default to `go`. Do not use both runtimes to consume the same shard concurrently: the existing lease/fencing protocol is the ownership boundary, not a language-specific process flag.

## Compatibility gate

The Rust runtime is intentionally fail-closed and **not production-ready** in this initial scaffold. It connects to NATS to validate configuration/connectivity, then exits without creating a consumer or acknowledging messages. This prevents a partial implementation from silently weakening the Go worker's guarantees.

Before enabling the Rust runtime, implement and test all of the following against the current Go implementation:

- Exact shared EventEnvelope JSON contract and validation behavior.
- The exact tenant/partition-key shard hash and configured shard-count behavior.
- Lease acquisition, renewal, release, monotonic fencing tokens, and stale-owner rejection.
- Transactional inbox deduplication, rule revision selection/evaluation, execution persistence, and outbox writes.
- ACK only after durable transaction commit; NAK/retry on transient failure; quarantine deterministic failures.
- Bounded concurrency and per-key ordering.
- Outbox claims, stable idempotency keys, retries, settlement, and quarantine.
- Scheduler claims and event publication, if the Rust runtime is expected to replace the complete Go worker.
- PostgreSQL and MongoDB behavior parity, or an explicit supported-backend restriction.
- Integration and failure-injection tests with real PostgreSQL and NATS, including crash-after-commit-before-ACK and stale lease owners.

## Local checks

```sh
cd rust-worker
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
```

The initial runtime deliberately does not satisfy production worker parity. Keep Go selected until every gate above passes.
