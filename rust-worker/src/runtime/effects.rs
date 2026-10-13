//! Outbox claiming, destination routing, and effect delivery.
use crate::{config::Config, engine, runtime::{validate_subject, uuid_token}};
use anyhow::{anyhow, bail, Context, Result};
use async_nats::jetstream;
use chrono::Utc;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

pub(super) async fn outbox_loop(pool: PgPool, js: jetstream::Context, config: Config) -> Result<()> {
    let http = reqwest::Client::builder().no_proxy().redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10)).build()?;
    let mut ticker = interval(config.effect_poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let claim = format!("{}-{}", config.worker_id, uuid_token());
        let rows = sqlx::query(
            "WITH candidates AS ( \
               SELECT effect_id FROM outbox_effects \
               WHERE (status='pending' AND available_at<=NOW()) OR (status='claimed' AND claim_expires_at<NOW()) \
               ORDER BY available_at,created_at LIMIT $1 FOR UPDATE SKIP LOCKED \
             ) UPDATE outbox_effects o SET status='claimed',claimed_by=$2,claimed_at=NOW(), \
               claim_expires_at=NOW()+INTERVAL '2 minutes',updated_at=NOW() \
             FROM candidates c WHERE o.effect_id=c.effect_id \
             RETURNING o.effect_id,o.execution_id,o.tenant_id,o.destination,o.name,o.payload,o.effect_type,o.attempts,o.max_attempts"
        ).bind(i64::try_from(config.fetch_batch_size)?).bind(&claim).fetch_all(&pool).await?;
        for row in rows {
            let id: String = row.try_get("effect_id")?;
            let execution_id: String = row.try_get("execution_id")?;
            let tenant_id: String = row.try_get("tenant_id")?;
            let destination: String = row.try_get("destination")?;
            let name: String = row.try_get("name")?;
            let payload: Value = row.try_get("payload")?;
            let effect_type: String = row.try_get("effect_type")?;
            let attempts: i32 = row.try_get("attempts")?;
            let max_attempts: i32 = row.try_get("max_attempts")?;
            let sent = send_effect(&pool, &js, &http, &config, &id, &execution_id, &tenant_id, &destination, &name, &payload, &effect_type).await;
            match sent {
                Ok(()) => {
                    let result = sqlx::query(
                        "UPDATE outbox_effects SET status='delivered',claimed_by=NULL,claimed_at=NULL,claim_expires_at=NULL,updated_at=NOW() \
                         WHERE effect_id=$1 AND status='claimed' AND claimed_by=$2 AND claim_expires_at>NOW()"
                    ).bind(&id).bind(&claim).execute(&pool).await?;
                    if result.rows_affected()!=1 { warn!(effect_id=%id,"effect delivered but claim settlement was lost"); }
                }
                Err(err) => {
                    let next_attempt = attempts + 1;
                    if next_attempt >= max_attempts {
                        persist_quarantine(&pool, "effect", &id, Some(&tenant_id), payload.to_string().as_bytes(), &err.to_string()).await?;
                        sqlx::query(
                            "UPDATE outbox_effects SET status='quarantined',attempts=$3,last_error=$4,claimed_by=NULL,claimed_at=NULL,claim_expires_at=NULL,updated_at=NOW() \
                             WHERE effect_id=$1 AND status='claimed' AND claimed_by=$2 AND claim_expires_at>NOW()"
                        ).bind(&id).bind(&claim).bind(next_attempt).bind(err.to_string()).execute(&pool).await?;
                    } else {
                        let delay = (1_i64 << (next_attempt - 1).clamp(0, 5)).min(60);
                        sqlx::query(
                            "UPDATE outbox_effects SET status='pending',attempts=$3,last_error=$4,available_at=NOW()+($5 * INTERVAL '1 second'), \
                             claimed_by=NULL,claimed_at=NULL,claim_expires_at=NULL,updated_at=NOW() \
                             WHERE effect_id=$1 AND status='claimed' AND claimed_by=$2 AND claim_expires_at>NOW()"
                        ).bind(&id).bind(&claim).bind(next_attempt).bind(err.to_string()).bind(delay).execute(&pool).await?;
                    }
                }
            }
        }
    }
}

async fn send_effect(
    pool: &PgPool, js: &jetstream::Context, http: &reqwest::Client, config: &Config,
    id: &str, execution_id: &str, tenant_id: &str, destination: &str, name: &str, payload: &Value, effect_type: &str,
) -> Result<()> {
    match effect_type {
        "emit" | "command" => {
            send_external_effect(http, config, id, execution_id, tenant_id, destination, name, payload, effect_type).await
        }
        "emit_event" => {
            let event: EventEnvelope = serde_json::from_value(payload.clone()).context("decode child event")?;
            let shard = event.virtual_shard(config.num_shards)?;
            let subject = event_subject(&event.event_type, shard, &config.rulesets);
            js.publish(subject, serde_json::to_vec(&event)?.into()).await.context("publish child event to JetStream")?;
            Ok(())
        }
        "service_event" => {
            send_domain_service_effect(pool, http, config, id, execution_id, tenant_id, destination, name, payload).await
        },
        other => bail!("unsupported outbox effect type {other:?}"),
    }
}

async fn send_external_effect(http: &reqwest::Client, config: &Config, id: &str, execution_id: &str, tenant_id: &str, destination: &str, name: &str, payload: &Value, effect_type: &str) -> Result<()> {
    if config.effect_destination_url.trim().is_empty() {
        if matches!(config.app_env.to_ascii_lowercase().as_str(), "dev" | "development" | "test") {
            info!(effect_id = id, destination, name, "development fake destination accepted effect");
            return Ok(());
        }
        bail!("EFFECT_DESTINATION_URL is required for external effects");
    }
    let mut url = reqwest::Url::parse(&config.effect_destination_url).context("parse EFFECT_DESTINATION_URL")?;
    {
        let mut segments = url.path_segments_mut().map_err(|_| anyhow!("EFFECT_DESTINATION_URL cannot be a base URL"))?;
        segments.pop_if_empty();
        segments.push(destination);
        segments.push(name);
    }
    let effect = json!({ "id": id, "execution_id": execution_id, "tenant_id": tenant_id, "destination": destination, "name": name, "payload": payload, "effect_type": effect_type, "created_at": Utc::now() });
    let response = http.post(url).header("Content-Type", "application/json")
        .header("X-Effect-ID", id).header("X-Execution-ID", execution_id)
        .header("Idempotency-Key", format!("effect-{id}"))
        .json(&effect).send().await.context("send external effect")?;
    if !response.status().is_success() { bail!("external effect endpoint returned {}", response.status()); }
    Ok(())
}

async fn send_domain_service_effect(pool: &PgPool, http: &reqwest::Client, config: &Config, id: &str, execution_id: &str, tenant_id: &str, service_id: &str, event_type: &str, payload: &Value) -> Result<()> {
    let endpoint: String = sqlx::query_scalar("SELECT endpoint_url FROM tenant_domain_services WHERE tenant_id=$1 AND service_id=$2 AND active=TRUE")
        .bind(tenant_id).bind(service_id).fetch_optional(pool).await?
        .ok_or_else(|| anyhow!("active tenant domain service {service_id:?} was not found"))?;
    let url = reqwest::Url::parse(&endpoint).context("parse tenant service endpoint URL")?;
    let local_env = matches!(config.app_env.to_ascii_lowercase().as_str(), "dev" | "development" | "test");
    if url.scheme() != "https" && !local_env { bail!("tenant service endpoint must use HTTPS outside development/test"); }
    if url.host_str().is_none() || url.username() != "" || url.password().is_some() || url.fragment().is_some() {
        bail!("tenant service endpoint URL contains invalid authority components");
    }
    if !local_env {
        if let Some(host) = url.host_str() {
            if let Ok(ip) = host.parse::<std::net::IpAddr>() {
                let private = match ip {
                    std::net::IpAddr::V4(v4) => v4.is_private() || v4.is_loopback() || v4.is_link_local() || v4.is_unspecified(),
                    std::net::IpAddr::V6(v6) => v6.is_loopback() || v6.is_unspecified() || v6.is_unique_local() || v6.is_unicast_link_local(),
                };
                if private { bail!("tenant service endpoint resolves to a non-public IP literal"); }
            }
        }
    }
    let event_id = payload.get("id").and_then(Value::as_str).unwrap_or_default();
    let response = http.post(url).header("Content-Type", "application/json")
        .header("Idempotency-Key", format!("effect-{id}"))
        .header("X-FlowRule-Tenant-ID", tenant_id)
        .header("X-FlowRule-Service-ID", service_id)
        .header("X-FlowRule-Effect-ID", id)
        .header("X-FlowRule-Event-ID", event_id)
        .header("X-Effect-ID", id).header("X-Execution-ID", execution_id)
        .header("X-FlowRule-Event-Type", event_type)
        .json(payload).send().await.context("send tenant service event callback")?;
    if !response.status().is_success() { bail!("tenant service callback returned {}", response.status()); }
    Ok(())
}
