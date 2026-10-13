//! Rust worker runtime entrypoint.
//!
//! This binary deliberately fails closed until a production EventProcessor is wired.
//! Never ACK a delivery merely because it was deserialized or received.

mod runtime;

use std::{env, process::ExitCode, time::Duration};

use tracing::{error, info};

#[derive(Debug)]
struct Config {
    nats_url: String,
    stream: String,
    durable: String,
    subject: String,
    ack_wait: Duration,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
        let stream = env::var("NATS_STREAM").unwrap_or_else(|_| "flowrule".into());
        let durable = env::var("NATS_CONSUMER").unwrap_or_else(|_| "flowrule-rust-worker".into());
        let subject = env::var("RUST_WORKER_SUBJECT")
            .unwrap_or_else(|_| "events.shard.*".into());
        let ack_wait_ms = env::var("NATS_ACK_WAIT_MS")
            .unwrap_or_else(|_| "30000".into())
            .parse::<u64>()
            .map_err(|_| "NATS_ACK_WAIT_MS must be a positive integer".to_string())?;
        if ack_wait_ms == 0 {
            return Err("NATS_ACK_WAIT_MS must be greater than zero".into());
        }
        if stream.trim().is_empty() || durable.trim().is_empty() || subject.trim().is_empty() {
            return Err("NATS_STREAM, NATS_CONSUMER and RUST_WORKER_SUBJECT cannot be empty".into());
        }
        Ok(Self {
            nats_url,
            stream,
            durable,
            subject,
            ack_wait: Duration::from_millis(ack_wait_ms),
        })
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            error!(error = %err, "Rust worker stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = Config::from_env()?;
    info!(
        stream = %config.stream,
        durable = %config.durable,
        subject = %config.subject,
        ack_wait_ms = config.ack_wait.as_millis(),
        "Rust worker runtime starting"
    );

    // The runtime is intentionally not wired to production until event processing,
    // fencing, inbox/outbox transactions, and lease ownership have parity tests.
    // Fail closed rather than consume messages with weaker guarantees than Go.
    runtime::run(config.nats_url, config.stream, config.durable, config.subject).await
}
