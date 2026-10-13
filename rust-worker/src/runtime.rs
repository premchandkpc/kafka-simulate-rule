//! Runtime wiring: PostgreSQL fencing/transactions, JetStream consumers, outbox and scheduler.
use crate::{engine::{self, EventEnvelope, ProcessOutcome}, Config};
use anyhow::{anyhow, bail, Context, Result};
use async_nats::jetstream::{self, consumer::pull};
use chrono::Utc;
use futures_util::StreamExt;
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{collections::HashMap, fs, path::Path, time::Duration};
use tokio::{task::JoinSet, time::{interval, MissedTickBehavior}};
use tracing::{error, info, warn};

pub async fn run(config: Config) -> Result<()> {
    let pool = PgPoolOptions::new().max_connections(20).connect(&config.database_url)
        .await.context("connect PostgreSQL")?;
    sqlx::query("SELECT 1").execute(&pool).await.context("verify PostgreSQL")?;
    run_migrations(&pool, Path::new(&config.migrations_dir)).await?;
    let client = async_nats::connect(config.nats_url.clone()).await.context("connect NATS")?;
    let js = jetstream::new(client);
    let _stream = js.get_stream(&config.stream).await
        .with_context(|| format!("get JetStream stream {}", config.stream))?;

    let leases = acquire_initial_leases(&pool, &config).await?;
    if leases.is_empty() {
        bail_no_leases(&config)?;
    }
    info!(owned_shards = leases.len(), "Rust worker acquired shard leases");

    let mut tasks = JoinSet::new();
    for (shard, token) in leases.iter() {
        let subjects = subjects_for_shard(*shard, &config);
        for (subject, durable) in subjects {
            let pool = pool.clone();
            let js = js.clone();
            let config = config.clone();
            let shard = *shard;
            let token = *token;
            tasks.spawn(async move {
                if let Err(err) = consume_shard_subject(pool, js, config, shard, token, subject, durable).await {
                    error!(shard, error = %err, "shard consumer stopped");
                }
            });
        }
    }

    let outbox_pool = pool.clone();
    let outbox_js = js.clone();
    let outbox_config = config.clone();
    tasks.spawn(async move {
        if let Err(err) = outbox_loop(outbox_pool, outbox_js, outbox_config).await {
            error!(error = %err, "outbox publisher stopped");
        }
    });

    let scheduler_pool = pool.clone();
    let scheduler_js = js.clone();
    let scheduler_config = config.clone();
    tasks.spawn(async move {
        if let Err(err) = scheduler_loop(scheduler_pool, scheduler_js, scheduler_config).await {
            error!(error = %err, "scheduled-event publisher stopped");
        }
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("shutdown signal received"),
        result = tasks.join_next() => {
            if let Some(Err(err)) = result { warn!(error=%err, "runtime task join failed"); }
            warn!("a core runtime task exited; shutting down remaining tasks");
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    for shard in leases.keys() {
        if let Err(err) = engine::release_lease(&pool, *shard, &config.worker_id).await {
            warn!(shard, error=%err, "failed to release shard lease");
        }
    }
    pool.close().await;
    Ok(())
}

async fn run_migrations(pool: &PgPool, directory: &Path) -> Result<()> {
    let mut files = fs::read_dir(directory)
        .with_context(|| format!("read migrations directory {}", directory.display()))?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().map(|t| t.is_file()).unwrap_or(false))
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "sql"))
        .collect::<Vec<_>>();
    files.sort_by_key(|entry| entry.file_name());
    for entry in &files {
        let path = entry.path();
        let sql = fs::read_to_string(&path).with_context(|| format!("read migration {}", path.display()))?;
        sqlx::raw_sql(&sql).execute(pool).await.with_context(|| format!("execute migration {}", path.display()))?;
    }
    info!(migration_count = files.len(), "database migrations applied");
    Ok(())
}

fn bail_no_leases(config: &Config) -> Result<()> {
    Err(anyhow!("worker {} acquired no shards; check WORKER_REPLICAS and existing shard owners", config.worker_id))
}

async fn acquire_initial_leases(pool: &PgPool, config: &Config) -> Result<HashMap<u32, i64>> {
    let target = config.num_shards.div_ceil(config.worker_replicas);
    let start = engine::fnv1a32(config.worker_id.as_bytes()) % config.num_shards;
    let ttl_ms = i64::try_from(config.lease_ttl.as_millis())?;
    let mut leases = HashMap::with_capacity(target as usize);
    for offset in 0..config.num_shards {
        if leases.len() >= target as usize { break; }
        let shard = (start + offset) % config.num_shards;
        match engine::acquire_lease(pool, shard, &config.worker_id, ttl_ms).await {
            Ok(Some(token)) => { leases.insert(shard, token); }
            Ok(None) => {}
            Err(err) => warn!(shard, error=%err, "initial shard lease attempt failed"),
        }
    }
    Ok(leases)
}

fn subjects_for_shard(shard: u32, config: &Config) -> Vec<(String, String)> {
    let mut subjects = vec![(format!("events.shard.{shard}"),
        format!("{}-legacy-shard-{shard}", config.durable))];
    for rule_set in &config.rulesets {
        let token = engine::sha256_hex(rule_set.as_bytes());
        subjects.push((format!("events.ruleset.{}.shard.{shard}", token),
            format!("{}-rs-{}-shard-{shard}", config.durable, &token[..32])));
    }
    subjects
}

async fn consume_shard_subject(
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
                let envelope = match serde_json::from_slice::<EventEnvelope>(&message.payload) {
                    Ok(value) => value,
                    Err(err) => {
                        persist_quarantine(&pool, "event", "unknown", None, &message.payload, &err.to_string()).await?;
                        message.ack().await.map_err(|err| anyhow!("ack quarantined invalid JSON event: {err}"))?;
                        continue;
                    }
                };
                let computed_shard = envelope.virtual_shard(config.num_shards)?;
                if computed_shard != shard {
                    let err = format!("message routed to shard {shard} but envelope hashes to {computed_shard}");
                    persist_quarantine(&pool, "event", &envelope.id, Some(&envelope.tenant_id), &message.payload, &err).await?;
                    message.ack().await.map_err(|err| anyhow!("ack quarantined misrouted event: {err}"))?;
                    continue;
                }
                match engine::process_event(&pool, &envelope, &config.worker_id, fencing_token, config.num_shards).await {
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
                        persist_quarantine(&pool, "event", &envelope.id, Some(&envelope.tenant_id), &message.payload, &err.to_string()).await
                            .context("persist permanent event failure to quarantine")?;
                        message.ack().await.map_err(|err| anyhow!("ack quarantined permanent event: {err}"))?;
                    }
                    Err(err) => {
                        warn!(shard, event_id=%envelope.id, error=%err, "event processing failed; scheduling retry");
                        message.ack_with(jetstream::AckKind::Nak(Some(config.retry_delay)))
                            .await.map_err(|err| anyhow!("nak failed event for retry: {err}"))?;
                    }
                }
            }
        }
    }
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

async fn persist_quarantine(
    pool: &PgPool,
    source_type: &str,
    source_id: &str,
    tenant_id: Option<&str>,
    raw: &[u8],
    error: &str,
) -> Result<()> {
    let quarantine_id = engine::sha256_hex(format!("{source_type}|{source_id}|{}", engine::sha256_hex(raw)).as_bytes());
    sqlx::query(
        "INSERT INTO quarantine (quarantine_id,source_type,source_id,error_class,payload_ref,event_id,tenant_id,error_message,created_at,updated_at) \
         VALUES ($1,$2,$3,'permanent','nats:flowrule',CASE WHEN $2='event' THEN $3 ELSE NULL END,$4,$5,NOW(),NOW()) ON CONFLICT (quarantine_id) DO NOTHING"
    ).bind(quarantine_id).bind(source_type).bind(source_id).bind(tenant_id).bind(error)
     .execute(pool).await.context("persist quarantine record")?;
    Ok(())
}

async fn outbox_loop(pool: PgPool, js: jetstream::Context, config: Config) -> Result<()> {
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
    if config.effect_destination_url.trim().is_empty() { bail!("EFFECT_DESTINATION_URL is required for external effects"); }
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

async fn scheduler_loop(pool: PgPool, js: jetstream::Context, config: Config) -> Result<()> {
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

fn event_subject(event_type: &str, shard: u32, rulesets: &[String]) -> String {
    if rulesets.iter().any(|r| r == event_type) {
        format!("events.ruleset.{}.shard.{shard}", engine::sha256_hex(event_type.as_bytes()))
    } else { format!("events.shard.{shard}") }
}

fn validate_subject(subject: &str) -> Result<()> {
    if subject.trim().is_empty() || subject.split('.').any(|part| part.is_empty() || (part.contains('*') || part.contains('>'))) {
        bail!("invalid concrete NATS subject");
    }
    Ok(())
}

fn uuid_token() -> String {
    format!("{}-{}", Utc::now().timestamp_nanos_opt().unwrap_or_default(), std::process::id())
}
