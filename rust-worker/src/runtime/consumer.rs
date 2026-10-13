//! JetStream event consumption and delivery settlement.
use crate::{config::Config, engine::{self, EventEnvelope, ProcessOutcome}, runtime::quarantine};
use anyhow::{anyhow, bail, Context, Result};
use async_nats::jetstream::{self, consumer::pull};
use futures_util::StreamExt;
use sqlx::PgPool;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

pub(super) fn subjects_for_shard(shard: u32, config: &Config) -> Vec<(String, String)> {
    let mut subjects = vec![(format!("events.shard.{shard}"),
        format!("{}-legacy-shard-{shard}", config.durable))];
    for rule_set in &config.rulesets {
        let token = engine::sha256_hex(rule_set.as_bytes());
        subjects.push((format!("events.ruleset.{}.shard.{shard}", token),
            format!("{}-rs-{}-shard-{shard}", config.durable, &token[..32])));
    }
    subjects
}

pub(super) async fn consume_shard_subject(
    pool: PgPool,
    js: jetstream::Context,
    config: Config,
    shard: u32,
    fencing_token: i64,
    subject: String,
    durable: String,
) -> Result<()> {
    let stream = js.get_stream(&config.stream).await?;
    let consumer = stream.get_or_create_consumer(&durable, pull::Config {
        durable_name: Some(durable.clone()),
        filter_subject: subject.clone(),
        ack_wait: config.ack_wait,
        max_deliver: -1,
        ..Default::default()
    }).await.with_context(|| format!("create durable {durable} for {subject}"))?;
    let mut messages = consumer.messages().await.context("open pull consumer message stream")?;
    let mut renew = interval(config.lease_renew_interval);
    renew.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = renew.tick() => {
                let ttl_ms = i64::try_from(config.lease_ttl.as_millis())?;
                if !engine::renew_lease(&pool, shard, &config.worker_id, fencing_token, ttl_ms).await? {
                    bail!("shard lease lost; consumer must stop");
                }
            }
            maybe_message = messages.next() => {
                let Some(message_result) = maybe_message else { bail!("JetStream consumer stream ended"); };
                let message = message_result.context("receive JetStream message")?;
                handle_message(&pool, message, &config, shard, fencing_token).await?;
            }
        }
    }
}

async fn handle_message(
    pool: &PgPool,
    message: jetstream::Message,
    config: &Config,
    shard: u32,
    fencing_token: i64,
) -> Result<()> {
    let envelope = match serde_json::from_slice::<EventEnvelope>(&message.payload) {
        Ok(value) => value,
        Err(err) => {
            quarantine_and_ack(pool, &message, "unknown", None, &err.to_string()).await?;
            return Ok(());
        }
    };

    if let Err(err) = envelope.validate() {
        quarantine_and_ack(pool, &message, &envelope.id, Some(&envelope.tenant_id), &err.to_string()).await?;
        return Ok(());
    }

    let computed_shard = envelope.virtual_shard(config.num_shards)?;
    if computed_shard != shard {
        let err = format!("message routed to shard {shard} but envelope hashes to {computed_shard}");
        quarantine_and_ack(pool, &message, &envelope.id, Some(&envelope.tenant_id), &err).await?;
        return Ok(());
    }

    match engine::process_event(pool, &envelope, &config.worker_id, fencing_token, config.num_shards).await {
        Ok(ProcessOutcome::Processed { execution_id, decision_hash, matched_rules }) => {
            info!(shard, event_id=%envelope.id, execution_id=%execution_id, decision_hash=%decision_hash,
                matched_rules=?matched_rules, "event committed");
            message.ack().await.map_err(|err| anyhow!("ack committed event: {err}"))?;
        }
        Ok(ProcessOutcome::Duplicate { execution_id }) => {
            info!(shard, event_id=%envelope.id, execution_id=%execution_id, "duplicate event confirmed");
            message.ack().await.map_err(|err| anyhow!("ack confirmed duplicate: {err}"))?;
        }
        Err(err) if is_permanent_event_error(&err) => {
            quarantine_and_ack(pool, &message, &envelope.id, Some(&envelope.tenant_id), &err.to_string()).await?;
        }
        Err(err) => {
            warn!(shard, event_id=%envelope.id, error=%err, "event processing failed; scheduling retry");
            message.ack_with(jetstream::AckKind::Nak(Some(config.retry_delay)))
                .await.map_err(|err| anyhow!("nak failed event for retry: {err}"))?;
        }
    }
    Ok(())
}

async fn quarantine_and_ack(
    pool: &PgPool,
    message: &jetstream::Message,
    event_id: &str,
    tenant_id: Option<&str>,
    reason: &str,
) -> Result<()> {
    quarantine::persist_quarantine(pool, "event", event_id, tenant_id, &message.payload, reason).await?;
    message.ack().await.map_err(|err| anyhow!("ack quarantined event: {err}"))?;
    Ok(())
}

fn is_permanent_event_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    ["event id is required", "event type is required", "tenant_id is required",
     "partition_key is required", "unsupported predicate operator", "unsupported partition key policy",
     "emit topic is required", "resolved an empty partition key",
     "action must contain exactly one supported action", "predicate path missing",
     "inbox duplicate has no committed execution", "inbox points to missing execution",
     "exceeds the 100-service fan-out limit", "command destination and name are required"].iter().any(|needle| message.contains(needle))
}
