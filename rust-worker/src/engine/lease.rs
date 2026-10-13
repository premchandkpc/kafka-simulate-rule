//! PostgreSQL shard lease acquisition, renewal, and release.
use anyhow::Result;
use sqlx::{PgPool, Row};

pub(super) async fn acquire_lease(pool: &PgPool, shard: u32, owner: &str, ttl_ms: i64) -> Result<Option<i64>> {
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

pub(super) async fn renew_lease(pool: &PgPool, shard: u32, owner: &str, token: i64, ttl_ms: i64) -> Result<bool> {
    let affected = sqlx::query(
        "UPDATE shard_leases SET expires_at=NOW()+($4 * INTERVAL '1 millisecond') \
         WHERE virtual_shard=$1 AND owner=$2 AND fencing_token=$3 AND expires_at>NOW()"
    ).bind(i32::try_from(shard)?).bind(owner).bind(token).bind(ttl_ms)
     .execute(pool).await?.rows_affected();
    Ok(affected == 1)
}

pub(super) async fn release_lease(pool: &PgPool, shard: u32, owner: &str) -> Result<()> {
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
