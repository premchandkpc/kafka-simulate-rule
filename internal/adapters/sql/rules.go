package sql

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/jackc/pgx/v5"
)

type RuleRepository struct {
	db ports.Querier
}

func NewRuleRepository(db ports.Querier) *RuleRepository {
	return &RuleRepository{db: db}
}

func (r *RuleRepository) GetActive(ctx context.Context, tenantScope string, ruleSet string) (*domain.RuleRevision, error) {
	row := r.db.QueryRow(ctx, `
		SELECT r.tenant_scope, r.rule_id, r.revision, r.source, r.compiled, r.content_hash, 
		       r.compiler_version, r.match_mode, r.created_at
		FROM rule_revisions r
		JOIN rule_activations a ON r.tenant_scope = a.tenant_scope AND r.rule_id = a.rule_set AND r.revision = a.revision
		WHERE a.tenant_scope = $1 AND a.rule_set = $2
	`, tenantScope, ruleSet)

	rev := &domain.RuleRevision{}
	var source, compiled []byte
	err := row.Scan(
		&rev.TenantScope, &rev.RuleID, &rev.Revision, &source, &compiled,
		&rev.ContentHash, &rev.CompilerVersion, &rev.MatchMode, &rev.CreatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query active revision: %w", err)
	}

	rev.Source = source
	if err := json.Unmarshal(compiled, &rev.Compiled); err != nil {
		return nil, fmt.Errorf("unmarshal compiled: %w", err)
	}
	return rev, nil
}

func (r *RuleRepository) Save(ctx context.Context, tenantScope string, revision *domain.RuleRevision) error {
	compiled, err := json.Marshal(revision.Compiled)
	if err != nil {
		return fmt.Errorf("marshal compiled: %w", err)
	}
	_, err = r.db.Exec(ctx, `
		INSERT INTO rule_revisions (tenant_scope, rule_id, revision, source, compiled, content_hash, compiler_version, match_mode, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (tenant_scope, rule_id, revision) DO NOTHING
	`, tenantScope, revision.RuleID, revision.Revision, revision.Source, compiled,
		revision.ContentHash, revision.CompilerVersion, revision.MatchMode, revision.CreatedAt)
	if err != nil {
		return fmt.Errorf("save revision: %w", err)
	}
	return nil
}

type ActivationRepository struct {
	db ports.Querier
}

func NewActivationRepository(db ports.Querier) *ActivationRepository {
	return &ActivationRepository{db: db}
}

func (a *ActivationRepository) Get(ctx context.Context, tenantScope string, ruleSet string) (*domain.RuleActivation, error) {
	act := &domain.RuleActivation{}
	err := a.db.QueryRow(ctx, `
		SELECT tenant_scope, rule_set, revision, version, actor, activated_at
		FROM rule_activations
		WHERE tenant_scope = $1 AND rule_set = $2
	`, tenantScope, ruleSet).Scan(
		&act.TenantScope, &act.RuleSet, &act.Revision, &act.Version, &act.Actor, &act.ActivatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get activation: %w", err)
	}
	return act, nil
}

func (a *ActivationRepository) Set(ctx context.Context, activation *domain.RuleActivation) error {
	_, err := a.db.Exec(ctx, `
		INSERT INTO rule_activations (tenant_scope, rule_set, revision, version, actor, activated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (tenant_scope, rule_set) DO UPDATE SET
			revision = EXCLUDED.revision,
			version = rule_activations.version + 1,
			actor = EXCLUDED.actor,
			activated_at = EXCLUDED.activated_at
	`, activation.TenantScope, activation.RuleSet, activation.Revision, activation.Version,
		activation.Actor, activation.ActivatedAt)
	if err != nil {
		return fmt.Errorf("set activation: %w", err)
	}
	return nil
}
