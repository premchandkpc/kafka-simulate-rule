//! Scheduled-event claiming, retry, and JetStream publication.
use crate::{config::Config, engine::{self, EventEnvelope}, runtime::uuid_token};
use anyhow::{Context, Result};
use async_nats::jetstream;
use chrono::Utc;
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::{collections::HashMap, time::Duration};
use tokio::time::{interval, MissedTickBehavior};

pub(super) async fn scheduler_loop(pool: PgPool, js: jetstream::Context, config: Config) -> Result<()> {
    let mut ticker = interval(config.scheduler_poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let claimant = format!("scheduler-{}", uuid_token());
        let rows = sqlx::query(
            "WITH due AS (SELECT event_id FROM scheduled_events \
             WHERE (status='pending' AND COALESCE(retry_at,scheduled_at)<=NOW()) OR (status='claimed' AND claim_expires_at<NOW()) \
             ORDER BY scheduled_at LIMIT $1 FOR UPDATE SKIP LOCKED) \
             UPDATE scheduled_events s SET status='claimed',claimant=$2,claimed_at=NOW(),claim_expires_at=NOW()+INTERVAL '5 minutes' \
             FROM due WHERE s.event_id=due.event_id \
             RETURNING s.event_id,s.tenant_id,s.event_type,s.partition_key,s.workflow_id,s.payload,s.headers,s.retry_count"
        ).bind(config.scheduler_batch_size).bind(&claimant).fetch_all(&pool).await?;
        for row in rows {
            let event_id: String = row.try_get("event_id")?;
            let tenant_id: String = row.try_get("tenant_id")?;
            let event_type: String = row.try_get("event_type")?;
            let partition_key: String = row.try_get("partition_key")?;
            let workflow_id: Option<String> = row.try_get("workflow_id")?;
            let payload: Value = row.try_get("payload")?;
            let headers: Option<Value> = row.try_get("headers")?;
            let retry_count: i32 = row.try_get("retry_count")?;
            let mut header_map = headers.and_then(|v| serde_json::from_value::<HashMap<String,String>>(v).ok()).unwrap_or_default();
            header_map.insert("Idempotency-Key".to_owned(), format!("sched-{event_id}"));
            let event = EventEnvelope { id: event_id.clone(), event_type: event_type.clone(), tenant_id, partition_key,
                workflow_id, occurred_at: Utc::now(), data: payload, headers: header_map };
            let send_result = async {
                let shard = event.virtual_shard(config.num_shards)?;
                let subject = event_subject(&event.event_type, shard, &config.rulesets);
                js.publish(subject, serde_json::to_vec(&event)?.into()).await.context("publish scheduled event to JetStream")?;
                Ok::<(), anyhow::Error>(())
            }.await;
            match send_result {
                Ok(()) => {
                    sqlx::query("UPDATE scheduled_events SET status='released',released_at=NOW(),claimant=NULL,claimed_at=NULL,claim_expires_at=NULL WHERE event_id=$1 AND claimant=$2 AND claim_expires_at>NOW()")
                        .bind(&event_id).bind(&claimant).execute(&pool).await?;
                }
                Err(err) => {
                    let next = retry_count + 1;
                    if next >= 8 {
                        sqlx::query("UPDATE scheduled_events SET status='failed',retry_count=$3,last_error=$4,claimant=NULL,claimed_at=NULL,claim_expires_at=NULL WHERE event_id=$1 AND claimant=$2 AND claim_expires_at>NOW()")
                            .bind(&event_id).bind(&claimant).bind(next).bind(err.to_string()).execute(&pool).await?;
                    } else {
                        let delay = (1_i64 << (next - 1).clamp(0, 5)).min(60);
                        sqlx::query("UPDATE scheduled_events SET status='pending',retry_count=$3,retry_at=NOW()+($4 * INTERVAL '1 second'),last_error=$5,claimant=NULL,claimed_at=NULL,claim_expires_at=NULL WHERE event_id=$1 AND claimant=$2 AND claim_expires_at>NOW()")
                            .bind(&event_id).bind(&claimant).bind(next).bind(delay).bind(err.to_string()).execute(&pool).await?;
                    }
                }
            }
        }
    }
}

pub(super) fn event_subject(event_type: &str, shard: u32, rulesets: &[String]) -> String {
    if rulesets.iter().any(|r| r == event_type) {
        format!("events.ruleset.{}.shard.{shard}", engine::sha256_hex(event_type.as_bytes()))
    } else { format!("events.shard.{shard}") }
}
