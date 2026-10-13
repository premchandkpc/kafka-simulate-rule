# Rust worker runtime (experimental)

## Goal

Provide a separately deployable Rust implementation alongside the existing Go worker. Go remains the default and its source is not removed or rewritten. Select the implementation with `WORKER_RUNTIME=go|rust`.

## Event and rule-set semantics

Rule-set lifecycle operations (compile, persist revision, activate/deactivate) are currently API/service operations backed by `rule_revisions` and `rule_activations`; the repository does not define a separate NATS rule-management event stream. Both workers process business events whose `EventEnvelope.type` identifies the rule set. The Rust worker reads the active revision from the same PostgreSQL tables.

The Rust runtime now has an initial end-to-end path for PostgreSQL deployments:

1. Applies the repository's idempotent SQL migration files.
2. Acquires shard leases with monotonically increasing fencing tokens and renews them while consuming.
3. Creates separate JetStream pull consumers for legacy `events.shard.N` subjects and configured `NATS_RULESETS` subjects (`events.ruleset.<sha256(type)>.shard.N`).
4. Validates the envelope and verifies the message's computed shard.
5. In one PostgreSQL transaction, checks lease ownership with `FOR SHARE`, inserts the inbox row, loads the active revision, evaluates the compiled rules, writes the execution and outbox effects, marks the inbox committed, and commits.
6. ACKs only after commit. Confirmed duplicates are ACKed; transient failures are NAKed with delay; malformed/permanent event failures are quarantined before ACK.
7. Claims and publishes outbox effects and due scheduled events with claim expiry, retry backoff, and terminal quarantine/failure state.

The Go rule API remains responsible for authoring and activating rule revisions. No independent Rust rule-authoring API or separate rule-set event protocol is introduced.

## Compatibility limits: do not treat this as production parity yet

The initial Rust engine is deliberately marked experimental and is not a drop-in replacement for every Go behavior. Known gaps include:

- PostgreSQL only; MongoDB is not supported by the Rust runtime.
- Rule evaluation still needs differential Go-vs-Rust tests and input-contract validation. The Rust runtime evaluates persisted compiled revisions directly; it does not provide a Rust rule-authoring/compiler API.
- Domain-service subscription fan-out and the Go effect router's destination/security policy are not yet equivalent. The Rust external command sender uses the configured `EFFECT_DESTINATION_URL`; review this contract before enabling command/service effects.
- Fixed replica count and initial lease acquisition are used. There is no dynamic worker membership/rebalancing protocol.
- No Rust equivalent of the Go worker's batching/key-queue backpressure, hot-key metrics, full observability/tracing, or all shutdown/recovery behavior.
- Integration tests against real PostgreSQL and NATS, crash-after-commit-before-ACK tests, and stale-owner failure injection are still required.

Keep Go selected in production until these gaps are closed and CI plus integration tests demonstrate behavioral parity. Do not run both runtimes against the same shard concurrently.

## Local checks

```sh
cd rust-worker
cargo fmt --all
cargo test
cargo clippy --all-targets -- -D warnings
```
