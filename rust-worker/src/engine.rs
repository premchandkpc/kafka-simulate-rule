//! Shared-domain Rust implementation for FlowRule business-event processing.
//! This module intentionally uses the existing PostgreSQL schema and event contract.
use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Deserialize)]
struct RuleRevision {
    tenant_scope: String,
    rule_id: String,
    revision: i64,
    content_hash: String,
    match_mode: String,
    compiled: Vec<Rule>,
}

#[derive(Debug, Deserialize)]
struct Rule {
    id: String,
    #[serde(default)]
    name: String,
    priority: i64,
    #[serde(rename = "when")]
    predicate: Predicate,
    #[serde(default)]
    then: Vec<Action>,
    #[serde(default)]
    otherwise: Vec<Action>,
}

#[derive(Debug, Deserialize)]
struct Predicate {
    #[serde(default)]
    all: Option<Vec<Predicate>>,
    #[serde(default)]
    any: Option<Vec<Predicate>>,
    #[serde(default)]
    not: Option<Box<Predicate>>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    op: Option<String>,
    #[serde(default)]
    value: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct Action {
    #[serde(default)]
    emit: Option<EmitAction>,
    #[serde(default)]
    emit_event: Option<EmitEventAction>,
    #[serde(default)]
    command: Option<CommandAction>,
}

#[derive(Debug, Deserialize)]
struct EmitAction { topic: String, data: Value }

#[derive(Debug, Deserialize)]
struct EmitEventAction {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    partition_key: String,
    #[serde(default)]
    partition_key_policy: String,
    #[serde(default)]
    partition_key_path: String,
    data: Value,
}

#[derive(Debug, Deserialize)]
struct CommandAction { destination: String, name: String, data: Value }

#[derive(Debug)]
struct Effect {
    id: String,
    tenant_id: String,
    destination: String,
    name: String,
    payload: Value,
    effect_type: &'static str,
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

fn evaluate(revision: &RuleRevision, event: &EventEnvelope) -> Result<(Vec<String>, Vec<Effect>)> {
    let mut matched = Vec::new();
    let mut effects = Vec::new();
    for rule in &revision.compiled {
        let is_match = eval_predicate(&rule.predicate, &event.data)?;
        let actions = if is_match { &rule.then } else { &rule.otherwise };
        if is_match { matched.push(rule.id.clone()); }
        for (index, action) in actions.iter().enumerate() {
            effects.push(build_effect(revision, event, &rule.id, index, action)?);
        }
        if revision.match_mode == "first_match" && is_match { break; }
    }
    Ok((matched, effects))
}

fn eval_predicate(predicate: &Predicate, data: &Value) -> Result<bool> {
    if let Some(children) = &predicate.all {
        for child in children { if !eval_predicate(child, data)? { return Ok(false); } }
        return Ok(true);
    }
    if let Some(children) = &predicate.any {
        for child in children { if eval_predicate(child, data)? { return Ok(true); } }
        return Ok(false);
    }
    if let Some(child) = &predicate.not { return Ok(!eval_predicate(child, data)?); }
    let path = predicate.path.as_deref().ok_or_else(|| anyhow!("predicate path missing"))?;
    let found = resolve_path(path, data);
    let op = predicate.op.as_deref().unwrap_or("");
    let expected = predicate.value.as_ref();
    if op == "exists" { return Ok(found.is_some()); }
    if op == "not_exists" { return Ok(found.is_none()); }
    let Some(actual) = found else { return Ok(false); };
    Ok(match op {
        "eq" => compare_eq(actual, expected),
        "neq" => !compare_eq(actual, expected),
        "gt" => compare(Some(actual), expected, |a,b| a > b),
        "gte" => compare(Some(actual), expected, |a,b| a >= b),
        "lt" => compare(Some(actual), expected, |a,b| a < b),
        "lte" => compare(Some(actual), expected, |a,b| a <= b),
        "in" => expected.and_then(Value::as_array).is_some_and(|xs| xs.iter().any(|x| compare_eq(actual, Some(x)))),
        "not_in" => !expected.and_then(Value::as_array).is_some_and(|xs| xs.iter().any(|x| compare_eq(actual, Some(x)))),
        _ => bail!("unsupported predicate operator {op:?}"),
    })
}

fn compare_eq(actual: &Value, expected: Option<&Value>) -> bool {
    let Some(expected) = expected else { return false; };
    match (number_value(actual), number_value(expected)) {
        (Some(a), Some(b)) => a == b,
        _ => actual == expected,
    }
}

fn number_value(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_str().and_then(|s| s.parse::<f64>().ok()))
}

fn compare(found: Option<&Value>, expected: Option<&Value>, predicate: impl Fn(f64, f64) -> bool) -> bool {
    match (found.and_then(number_value), expected.and_then(number_value)) {
        (Some(a), Some(b)) => predicate(a, b),
        _ => false,
    }
}

fn resolve_path<'a>(path: &str, root: &'a Value) -> Option<&'a Value> {
    let trimmed = path.strip_prefix("$.").unwrap_or(path);
    if trimmed.is_empty() || trimmed == "facts" || trimmed.starts_with("facts.") { return None; }
    trimmed.split('.').try_fold(root, |value, key| {
        if let Ok(index) = key.parse::<usize>() { value.as_array()?.get(index) }
        else { value.as_object()?.get(key) }
    })
}

fn interpolate_templates(value: &Value, event: &EventEnvelope, child_id: &str) -> Value {
    match value {
        Value::String(text) => expand_template(text, event, child_id),
        Value::Array(items) => Value::Array(items.iter().map(|item| interpolate_templates(item, event, child_id)).collect()),
        Value::Object(object) => {
            let mut result = serde_json::Map::new();
            for (key, item) in object { result.insert(key.clone(), interpolate_templates(item, event, child_id)); }
            Value::Object(result)
        }
        other => other.clone(),
    }
}

fn expand_template(text: &str, event: &EventEnvelope, child_id: &str) -> Value {
    static PLACEHOLDER: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"\{\{\s*([^{}]+?)\s*\}\}").expect("static template regex"));
    if !text.contains("{{") { return Value::String(text.to_owned()); }
    if let Some(captures) = PLACEHOLDER.captures(text) {
        if captures.get(0).is_some_and(|whole| whole.as_str() == text) {
            return template_lookup(captures.get(1).map(|m| m.as_str()).unwrap_or_default(), event, child_id).unwrap_or(Value::String(String::new()));
        }
    }
    let rendered = PLACEHOLDER.replace_all(text, |captures: &regex::Captures<'_>| {
        match template_lookup(captures.get(1).map(|m| m.as_str()).unwrap_or_default(), event, child_id) {
            Some(Value::String(value)) => value,
            Some(value) => value.to_string(),
            None => String::new(),
        }
    });
    Value::String(rendered.into_owned())
}

fn template_lookup(expression: &str, event: &EventEnvelope, child_id: &str) -> Option<Value> {
    match expression.trim() {
        "new_id" => return Some(Value::String(child_id.to_owned())),
        "now" => return Some(Value::String(Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true))),
        _ => {},
    }
    let path = expression.trim();
    let path = if path.starts_with("$.") { path.to_owned() } else { format!("$.{path}") };
    resolve_path(&path, &event.data).cloned()
}

fn build_effect(revision: &RuleRevision, event: &EventEnvelope, rule_id: &str, index: usize, action: &Action) -> Result<Effect> {
    let effect_id = sha256_hex(format!(
        "{}|{}|{}|{}|{}|{}",
        event.tenant_id, event.id, revision.rule_id, revision.revision, rule_id, index
    ).as_bytes());
    if let Some(emit) = &action.emit {
        if emit.topic.trim().is_empty() { bail!("emit topic is required"); }
        return Ok(Effect { id: effect_id, tenant_id: event.tenant_id.clone(), destination: emit.topic.clone(),
            name: emit.topic.clone(), payload: emit.data.clone(), effect_type: "emit" });
    }
    if let Some(child_action) = &action.emit_event {
        let child_id = sha256_hex(format!("child|{}|{}|{}", event.id, rule_id, index).as_bytes());
        let policy = if child_action.partition_key_policy.is_empty() { "explicit" } else { &child_action.partition_key_policy };
        let partition_key = match policy {
            "explicit" => child_action.partition_key.clone(),
            "inherit" => event.partition_key.clone(),
            "from_data" => resolve_path(&child_action.partition_key_path, &event.data)
                .map(|v| v.as_str().map(str::to_owned).unwrap_or_else(|| v.to_string()))
                .unwrap_or_default(),
            _ => bail!("unsupported partition key policy {policy:?}"),
        };
        if partition_key.is_empty() { bail!("emit_event resolved an empty partition key"); }
        let payload = json!({
            "id": child_id,
            "type": child_action.event_type,
            "tenant_id": event.tenant_id,
            "partition_key": partition_key,
            "occurred_at": Utc::now(),
            "data": interpolate_templates(&child_action.data, event, &child_id),
            "headers": {
                "flowrule_parent_event_id": event.id,
                "flowrule_child_event_id": child_id
            }
        });
        return Ok(Effect { id: effect_id, tenant_id: event.tenant_id.clone(), destination: format!("events.{}", child_action.event_type),
            name: child_action.event_type.clone(), payload, effect_type: "emit_event" });
    }
    if let Some(command) = &action.command {
        if command.destination.trim().is_empty() || command.name.trim().is_empty() { bail!("command destination and name are required"); }
        return Ok(Effect { id: effect_id, tenant_id: event.tenant_id.clone(), destination: command.destination.clone(),
            name: command.name.clone(), payload: command.data.clone(), effect_type: "command" });
    }
    bail!("action must contain exactly one supported action")
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}

pub async fn acquire_lease(pool: &PgPool, shard: u32, owner: &str, ttl_ms: i64) -> Result<Option<i64>> {
    let row = sqlx::query(
        "INSERT INTO shard_leases (virtual_shard,owner,fencing_token,expires_at,routing_epoch) \
         VALUES ($1,$2,1,NOW()+($3 * INTERVAL '1 millisecond'),0) \
         ON CONFLICT (virtual_shard) DO UPDATE SET \
           owner=CASE WHEN shard_leases.expires_at < NOW() THEN EXCLUDED.owner ELSE shard_leases.owner END, \
           fencing_token=CASE WHEN shard_leases.expires_at < NOW() THEN shard_leases.fencing_token+1 ELSE shard_leases.fencing_token END, \
           expires_at=CASE WHEN shard_leases.expires_at < NOW() THEN EXCLUDED.expires_at ELSE shard_leases.expires_at END \
         RETURNING owner,fencing_token,expires_at > NOW() AS active"
    ).bind(i32::try_from(shard)?).bind(owner).bind(ttl_ms).fetch_one(pool).await?;
    let lease_owner: String = row.try_get("owner")?;
    let token: i64 = row.try_get("fencing_token")?;
    let active: bool = row.try_get("active")?;
    Ok((lease_owner == owner && active).then_some(token))
}

pub async fn renew_lease(pool: &PgPool, shard: u32, owner: &str, token: i64, ttl_ms: i64) -> Result<bool> {
    let affected = sqlx::query(
        "UPDATE shard_leases SET expires_at=NOW()+($4 * INTERVAL '1 millisecond') \
         WHERE virtual_shard=$1 AND owner=$2 AND fencing_token=$3 AND expires_at>NOW()"
    ).bind(i32::try_from(shard)?).bind(owner).bind(token).bind(ttl_ms)
     .execute(pool).await?.rows_affected();
    Ok(affected == 1)
}

pub async fn release_lease(pool: &PgPool, shard: u32, owner: &str) -> Result<()> {
    sqlx::query("UPDATE shard_leases SET expires_at=NOW()-INTERVAL '1 millisecond' WHERE virtual_shard=$1 AND owner=$2")
        .bind(i32::try_from(shard)?).bind(owner).execute(pool).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
