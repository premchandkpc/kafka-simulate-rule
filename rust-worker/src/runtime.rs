//! Runtime boundary for the Rust worker.
//!
//! The production implementation must not be enabled until the Rust processor
//! shares Go's event envelope, shard hash, lease fencing, inbox/outbox transaction,
//! rule evaluation and effect settlement semantics.

use async_nats::jetstream;
use thiserror::Error;
use tracing::info;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("cannot connect to NATS: {0}")]
    Connect(#[from] async_nats::ConnectError),
    #[error("cannot initialize JetStream: {0}")]
    JetStream(#[from] async_nats::jetstream::context::CreateStreamError),
    #[error("Rust worker is not production-enabled: event processor, lease fencing, and transactional outbox parity are not implemented")]
    NotProductionReady,
}

pub async fn run(
    nats_url: String,
    stream: String,
    durable: String,
    subject: String,
) -> Result<(), RuntimeError> {
    // Validate connectivity while this alternative runtime is being developed.
    // Do not create a consumer or pull messages before processing parity exists.
    let client = async_nats::connect(nats_url).await?;
    let _js = jetstream::new(client);
    info!(stream, durable, subject, "NATS connection established; refusing to consume until parity gate is removed");
    Err(RuntimeError::NotProductionReady)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runtime_does_not_ack_or_consume_without_processor_parity() {
        let result = run(
            "nats://127.0.0.1:1".to_string(),
            "flowrule".to_string(),
            "flowrule-rust-worker".to_string(),
            "events.shard.*".to_string(),
        )
        .await;
        assert!(result.is_err());
    }
}
