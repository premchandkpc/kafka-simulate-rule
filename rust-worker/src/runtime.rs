//! Runtime boundary for the Rust worker.
//!
//! Deliberately fail closed until processing, fencing, and transaction parity is tested.

use anyhow::{bail, Result};
use tracing::info;

pub async fn run(nats_url: String, stream: String, durable: String, subject: String) -> Result<()> {
    // Verify connectivity, but do not create a durable consumer or pull messages yet.
    let client = async_nats::connect(nats_url).await?;
    let _jetstream = async_nats::jetstream::new(client);
    info!(stream, durable, subject, "NATS connection established; Rust processing parity gate is closed");
    bail!("Rust worker is not production-ready: event processing, lease fencing, and transactional inbox/outbox parity are not implemented")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unavailable_nats_fails_without_consuming_messages() {
        let result = run(
            "nats://127.0.0.1:1".to_string(),
            "flowrule".to_string(),
            "flowrule-rust-worker".to_string(),
            "events.shard.*".to_string(),
        ).await;
        assert!(result.is_err());
    }
}
