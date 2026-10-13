//! Shared-domain Rust implementation for FlowRule business-event processing.
//! This module intentionally uses the existing PostgreSQL schema and event contract.
use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use self::rules::{Effect, RuleRevision};

mod lease;
mod rules;
pub use lease::{acquire_lease, release_lease, renew_lease};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction, Row};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventEnvelope {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub tenant_id: String,
    pub partition_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub data: Value,
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub headers: std::collections::HashMap<String, String>,
}

impl EventEnvelope {
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() { bail!("event id is required"); }
        if self.event_type.trim().is_empty() { bail!("event type is required"); }
        if self.tenant_id.trim().is_empty() { bail!("tenant_id is required"); }
        if self.partition_key.trim().is_empty() { bail!("partition_key is required"); }
        Ok(())
    }

    pub fn virtual_shard(&self, num_shards: u32) -> Result<u32> {
        if num_shards == 0 { bail!("NUM_SHARDS must be greater than zero"); }
        let key = format!("{}:{}", self.tenant_id, self.partition_key);
        Ok(fnv1a32(key.as_bytes()) % num_shards)
    }
}

pub fn fnv1a32(bytes: &[u8]) -> u32 {
    let mut hash = 2_166_136_261_u32;
    for byte in bytes { hash ^= u32::from(*byte); hash = hash.wrapping_mul(16_777_619); }
    hash
}

pub enum ProcessOutcome {
    Processed { execution_id: String, decision_hash: String, matched_rules: Vec<String> },
    Duplicate { execution_id: String },
}

pub async fn process_event(
    pool: &PgPool,
    envelope: &EventEnvelope,
    worker_id: &str,
    fencing_token: i64,
    num_shards: u32,
) -> Result<ProcessOutcome> {
    envelope.validate()?;
    if worker_id.trim().is_empty() || fencing_token <= 0 { bail!("worker id and positive fencing token are required"); }
    let shard = envelope.virtual_shard(num_shards)?;
    let mut tx = pool.begin().await.context("begin event transaction")?;
    assert_lease(&mut tx, shard, worker_id, fencing_token).await?;

    let inserted = sqlx::query(
        "INSERT INTO inbox (tenant_id,event_id,status,first_seen_at,partition_key,rule_set,payload) \
         VALUES ($1,$2,'processing',NOW(),$3,$4,$5) ON CONFLICT (tenant_id,event_id) DO NOTHING"
    ).bind(&envelope.tenant_id).bind(&envelope.id).bind(&envelope.partition_key)
     .bind(&envelope.event_type).bind(&envelope.data)
     .execute(&mut *tx).await.context("insert inbox row")?.rows_affected() == 1;

    if !inserted {
        let execution_id: Option<String> = sqlx::query_scalar(
            "SELECT execution_id FROM inbox WHERE tenant_id=$1 AND event_id=$2"
        ).bind(&envelope.tenant_id).bind(&envelope.id).fetch_optional(&mut *tx).await?
         .flatten();
        let Some(execution_id) = execution_id else {
            bail!("inbox duplicate has no committed execution; refusing unsafe acknowledgement");
        };
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM executions WHERE execution_id=$1)")
            .bind(&execution_id).fetch_one(&mut *tx).await?;
        if !exists { bail!("inbox points to missing execution; refusing unsafe acknowledgement"); }
        tx.commit().await.context("commit duplicate acknowledgement transaction")?;
        return Ok(ProcessOutcome::Duplicate { execution_id });
    }

    let row = sqlx::query(
        "SELECT r.tenant_scope,r.rule_id,r.revision,r.content_hash,r.match_mode,r.compiled \
         FROM rule_revisions r JOIN rule_activations a \
         ON a.tenant_scope=r.tenant_scope AND a.rule_set=r.rule_id AND a.revision=r.revision \
         WHERE a.tenant_scope=$1 AND a.rule_set=$2"
    ).bind(&envelope.tenant_id).bind(&envelope.event_type)
     .fetch_optional(&mut *tx).await.context("load active rule revision")?
     .ok_or_else(|| anyhow!("active rule revision not found for tenant={} rule_set={}", envelope.tenant_id, envelope.event_type))?;
    let revision = RuleRevision {
        tenant_scope: row.try_get("tenant_scope")?,
        rule_id: row.try_get("rule_id")?,
        revision: row.try_get("revision")?,
        content_hash: row.try_get("content_hash")?,
        match_mode: row.try_get("match_mode")?,
        compiled: serde_json::from_value(row.try_get::<Value, _>("compiled")?)
            .context("decode compiled rule set")?,
    };
    let (matched, mut effects) = evaluate(&revision, envelope)?;
    let mut decision_material = revision.content_hash.clone();
    for id in &matched { decision_material.push_str(id); }
    for effect in &effects { decision_material.push_str(&effect.id); }
    decision_material.push_str(&envelope.id);
    let decision_hash = sha256_hex(decision_material.as_bytes());
    effects.extend(load_service_subscriber_effects(&mut tx, envelope).await?);
    let execution_id = sha256_hex(format!(
        "execution|{}:{}|{}:{}|{}:{}|{}",
        envelope.tenant_id.len(), envelope.tenant_id,
        envelope.id.len(), envelope.id,
        envelope.event_type.len(), envelope.event_type,
        revision.revision
    ).as_bytes());

    sqlx::query(
        "INSERT INTO executions (execution_id,event_id,tenant_id,rule_set,revision,decision_hash,status,created_at,completed_at) \
         VALUES ($1,$2,$3,$4,$5,$6,'completed',NOW(),NOW())"
    ).bind(&execution_id).bind(&envelope.id).bind(&envelope.tenant_id)
     .bind(&envelope.event_type).bind(revision.revision).bind(&decision_hash)
     .execute(&mut *tx).await.context("persist execution")?;

    for effect in effects {
        sqlx::query(
            "INSERT INTO outbox_effects (effect_id,execution_id,tenant_id,destination,name,payload,effect_type,status,attempts,max_attempts,available_at,created_at,updated_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,'pending',0,5,NOW(),NOW(),NOW()) \
             ON CONFLICT (effect_id) DO NOTHING"
        ).bind(effect.id).bind(&execution_id).bind(effect.tenant_id).bind(effect.destination)
         .bind(effect.name).bind(effect.payload).bind(effect.effect_type)
         .execute(&mut *tx).await.context("persist outbox effect")?;
    }

    let affected = sqlx::query(
        "UPDATE inbox SET status='committed',execution_id=$3,committed_at=NOW() WHERE tenant_id=$1 AND event_id=$2 AND status='processing'"
    ).bind(&envelope.tenant_id).bind(&envelope.id).bind(&execution_id)
     .execute(&mut *tx).await.context("mark inbox committed")?.rows_affected();
    if affected != 1 { bail!("inbox commit transition affected {affected} rows"); }
    tx.commit().await.context("commit event transaction")?;
    Ok(ProcessOutcome::Processed { execution_id, decision_hash, matched_rules: matched })
}

async fn assert_lease(tx: &mut Transaction<'_, Postgres>, shard: u32, owner: &str, token: i64) -> Result<()> {
    let row = sqlx::query(
        "SELECT fencing_token, expires_at > NOW() AS active FROM shard_leases \
         WHERE virtual_shard=$1 AND owner=$2 FOR SHARE"
    ).bind(i32::try_from(shard)?).bind(owner).fetch_optional(&mut **tx).await?
     .ok_or_else(|| anyhow!("shard lease is not held by this worker"))?;
    let current: i64 = row.try_get("fencing_token")?;
    let active: bool = row.try_get("active")?;
    if !active || token <= 0 || current != token { bail!("shard lease expired or fencing token mismatch"); }
    Ok(())
}

async fn load_service_subscriber_effects(tx: &mut Transaction<'_, Postgres>, event: &EventEnvelope) -> Result<Vec<Effect>> {
    let rows = sqlx::query("SELECT s.service_id FROM tenant_domain_services s JOIN tenant_domain_subscriptions sub ON sub.tenant_id=s.tenant_id AND sub.service_id=s.service_id JOIN tenant_domains d ON d.tenant_id=sub.tenant_id AND d.domain_id=sub.domain_id WHERE s.tenant_id=$1 AND s.active=TRUE AND $2=ANY(d.event_types) ORDER BY s.service_id LIMIT 101")
        .bind(&event.tenant_id).bind(&event.event_type).fetch_all(&mut **tx).await
        .context("load tenant domain-service subscribers")?;
    if rows.len() > 100 { bail!("event type {} exceeds the 100-service fan-out limit", event.event_type); }
    let payload = serde_json::to_value(event).context("serialize subscribed event envelope")?;
    let mut effects = Vec::with_capacity(rows.len());
    for row in rows {
        let service_id: String = row.try_get("service_id")?;
        let effect_id = sha256_hex(format!("{}|{}|service-subscription|1|{}|0", event.tenant_id, event.id, service_id).as_bytes());
        effects.push(Effect { id: effect_id, tenant_id: event.tenant_id.clone(), destination: service_id, name: event.event_type.clone(), payload: payload.clone(), effect_type: "service_event" });
    }
    Ok(effects)
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::rules::*;

    #[test]
    fn shard_hash_matches_fnv1a_contract() {
        assert_eq!(fnv1a32(b""), 2_166_136_261);
        assert_eq!(fnv1a32(b"a"), 3_826_002_220);
    }

    #[test]
    fn evaluates_nested_predicates_and_paths() {
        let predicate: Predicate = serde_json::from_value(json!({
            "all": [
                {"path":"$.amount","op":"gte","value":10},
                {"path":"$.status","op":"eq","value":"ready"}
            ]
        })).unwrap();
        assert!(eval_predicate(&predicate, &json!({"amount":10,"status":"ready"})).unwrap());
        assert!(!eval_predicate(&predicate, &json!({"amount":9,"status":"ready"})).unwrap());
    }

    #[test]
    fn rejects_empty_partition_key_for_child_event() {
        let revision = RuleRevision { tenant_scope:"t".into(), rule_id:"r".into(), revision:1, content_hash:"h".into(), match_mode:"all_matches".into(), compiled:vec![] };
        let event = EventEnvelope { id:"e".into(), event_type:"r".into(), tenant_id:"t".into(), partition_key:"p".into(), workflow_id:None, occurred_at:Utc::now(), data:json!({}), headers:Default::default() };
        let action: Action = serde_json::from_value(json!({"emit_event":{"type":"child","partition_key_policy":"from_data","partition_key_path":"$.missing","data":{}}})).unwrap();
        assert!(build_effect(&revision, &event, "rule", 0, &action).is_err());
    }
    #[test]
    fn templates_preserve_single_placeholder_type_and_expand_nested_strings() {
        let event = EventEnvelope { id: "e".into(), event_type: "orders".into(), tenant_id: "t".into(),
            partition_key: "p".into(), workflow_id: None, occurred_at: Utc::now(),
            data: json!({"amount": 12, "customer": {"name": "Ada"}}), headers: Default::default() };
        assert_eq!(expand_template("{{$.amount}}", &event, "child-1"), json!(12));
        assert_eq!(expand_template("amount={{$.amount}} for {{$.customer.name}}", &event, "child-1"), json!("amount=12 for Ada"));
        assert_eq!(expand_template("{{new_id}}", &event, "child-1"), json!("child-1"));
    }

    #[test]
    fn numeric_string_comparison_matches_go_rules() {
        assert!(compare_eq(&json!(10), Some(&json!("10"))));
        assert!(!eval_predicate(&Predicate { all: None, any: None, not: None, path: Some("$.missing".into()), op: Some("neq".into()), value: Some(json!(1)) }, &json!({})).unwrap());
    }

}
