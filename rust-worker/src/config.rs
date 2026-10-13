//! Environment-backed configuration for the Rust worker runtime.
use anyhow::{bail, Result};
use std::{env, time::Duration};

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) app_env: String,
    pub(crate) nats_url: String,
    pub(crate) stream: String,
    pub(crate) durable: String,
    pub(crate) database_url: String,
    pub(crate) migrations_dir: String,
    pub(crate) worker_id: String,
    pub(crate) num_shards: u32,
    pub(crate) worker_replicas: u32,
    pub(crate) lease_ttl: Duration,
    pub(crate) lease_renew_interval: Duration,
    pub(crate) ack_wait: Duration,
    pub(crate) retry_delay: Duration,
    pub(crate) fetch_batch_size: usize,
    pub(crate) rulesets: Vec<String>,
    pub(crate) effect_destination_url: String,
    pub(crate) effect_poll_interval: Duration,
    pub(crate) scheduler_poll_interval: Duration,
    pub(crate) scheduler_batch_size: i64,
}

impl Config {
    pub(crate) fn from_env() -> Result<Self> {
        let app_env = env::var("APP_ENV").unwrap_or_else(|_| "production".into());
        let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
        let stream = env::var("NATS_STREAM").unwrap_or_else(|_| "flowrule".into());
        let durable = env::var("NATS_CONSUMER").unwrap_or_else(|_| "flowrule-worker".into());
        let storage_backend = env::var("STORAGE_BACKEND").unwrap_or_else(|_| "postgres".into());
        if !storage_backend.eq_ignore_ascii_case("postgres") {
            bail!("Rust worker supports STORAGE_BACKEND=postgres only; MongoDB parity is not implemented");
        }
        let database_url = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:5432/flowrule?sslmode=disable".into());
        let migrations_dir = env::var("MIGRATIONS_DIR").unwrap_or_else(|_| "migrations".into());
        let worker_id = env::var("WORKER_ID").unwrap_or_else(|_| {
            format!("rust-{}", std::env::var("HOSTNAME").unwrap_or_else(|_| "worker".into()))
        });
        let num_shards = parse_u32("NUM_SHARDS", 4096)?;
        let worker_replicas = parse_u32("WORKER_REPLICAS", 1)?;
        let lease_ttl = Duration::from_millis(parse_u64("LEASE_TTL_MS", 30_000)?);
        let lease_renew_interval = Duration::from_millis(parse_u64("LEASE_RENEWAL_INTERVAL_MS", 10_000)?);
        let ack_wait = Duration::from_millis(parse_u64("NATS_ACK_WAIT_MS", 30_000)?);
        let retry_delay = Duration::from_millis(parse_u64("EVENT_RETRY_DELAY_MS", 5_000)?);
        let fetch_batch_size = parse_u64("FETCH_BATCH_SIZE", 100)? as usize;
        let rulesets: Vec<String> = env::var("NATS_RULESETS").unwrap_or_default().split(',')
            .map(str::trim).filter(|s| !s.is_empty()).map(str::to_owned).collect();
        let effect_destination_url = env::var("EFFECT_DESTINATION_URL").unwrap_or_default();
        let effect_poll_interval = Duration::from_millis(parse_u64("EFFECT_PUBLISH_INTERVAL_MS", 5_000)?);
        let scheduler_poll_interval = Duration::from_millis(parse_u64("SCHEDULER_POLL_MS", 5_000)?);
        let scheduler_batch_size = parse_u64("SCHEDULER_BATCH_SIZE", 100)? as i64;

        if stream.trim().is_empty() || durable.trim().is_empty() || worker_id.trim().is_empty() {
            bail!("NATS_STREAM, NATS_CONSUMER and WORKER_ID cannot be empty");
        }
        if database_url.trim().is_empty() { bail!("DATABASE_URL cannot be empty"); }
        if nats_url.trim().is_empty() { bail!("NATS_URL cannot be empty"); }
        if !matches!(app_env.to_ascii_lowercase().as_str(), "dev" | "development" | "test")
            && effect_destination_url.trim().is_empty() {
            bail!("EFFECT_DESTINATION_URL is required outside development/test");
        }
        let mut seen_rulesets = std::collections::HashSet::new();
        for ruleset in &rulesets {
            if ruleset.len() > 128 { bail!("NATS_RULESETS entries must be at most 128 bytes"); }
            if !seen_rulesets.insert(ruleset) { bail!("NATS_RULESETS contains duplicate rule set {ruleset:?}"); }
        }
        if num_shards == 0 || worker_replicas == 0 { bail!("NUM_SHARDS and WORKER_REPLICAS must be > 0"); }
        if lease_ttl.is_zero() || lease_renew_interval.is_zero() || lease_renew_interval * 3 > lease_ttl {
            bail!("LEASE_RENEWAL_INTERVAL_MS must be positive and no greater than one third of LEASE_TTL_MS");
        }
        if ack_wait.is_zero() || retry_delay.is_zero() || fetch_batch_size == 0 { bail!("NATS_ACK_WAIT_MS, EVENT_RETRY_DELAY_MS and FETCH_BATCH_SIZE must be > 0"); }
        Ok(Self { app_env, nats_url, stream, durable, database_url, migrations_dir, worker_id, num_shards, worker_replicas,
            lease_ttl, lease_renew_interval, ack_wait, retry_delay, fetch_batch_size, rulesets,
            effect_destination_url, effect_poll_interval, scheduler_poll_interval, scheduler_batch_size })
    }
}

fn parse_u64(name: &str, default: u64) -> Result<u64> {
    let value = env::var(name).unwrap_or_else(|_| default.to_string());
    let parsed = value.parse::<u64>().map_err(|e| anyhow::anyhow!("{name} must be an unsigned integer: {e}"))?;
    if parsed == 0 { bail!("{name} must be greater than zero"); }
    Ok(parsed)
}
fn parse_u32(name: &str, default: u32) -> Result<u32> {
    let value = parse_u64(name, u64::from(default))?;
    u32::try_from(value).map_err(|_| anyhow::anyhow!("{name} exceeds u32"))
}
