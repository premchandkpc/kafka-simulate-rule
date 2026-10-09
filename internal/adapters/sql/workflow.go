package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/jackc/pgx/v5"
)

type WorkflowRepository struct {
	db Querier
}

func NewWorkflowRepository(db Querier) *WorkflowRepository {
	return &WorkflowRepository{db: db}
}

func (r *WorkflowRepository) Save(ctx context.Context, workflow *domain.WorkflowInstance) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO workflow_instances (workflow_id, tenant_id, workflow_type, state, version, current_revision, context, created_at, updated_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (workflow_id) DO NOTHING
	`, workflow.WorkflowID, workflow.TenantID, workflow.WorkflowType, workflow.State, workflow.Version, workflow.CurrentRevision, workflow.Context, workflow.CreatedAt, workflow.UpdatedAt, workflow.CompletedAt)
	if err != nil {
		return fmt.Errorf("save workflow: %w", err)
	}
	return nil
}

func (r *WorkflowRepository) Get(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error) {
	w := &domain.WorkflowInstance{}
	var completedAt *time.Time
	err := r.db.QueryRow(ctx, `
		SELECT workflow_id, tenant_id, workflow_type, state, version, current_revision, context, created_at, updated_at, completed_at
		FROM workflow_instances
		WHERE workflow_id = $1
	`, workflowID).Scan(
		&w.WorkflowID, &w.TenantID, &w.WorkflowType, &w.State, &w.Version,
		&w.CurrentRevision, &w.Context, &w.CreatedAt, &w.UpdatedAt, &completedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get workflow: %w", err)
	}
	w.CompletedAt = completedAt
	return w, nil
}

func (r *WorkflowRepository) Update(ctx context.Context, workflow *domain.WorkflowInstance) error {
	now := time.Now().UTC()
	tag, err := r.db.Exec(ctx, `
		UPDATE workflow_instances
		SET state = $2, version = $3, current_revision = $4, context = $5, updated_at = $6, completed_at = $7
		WHERE workflow_id = $1 AND version = $8
	`, workflow.WorkflowID, workflow.State, workflow.Version, workflow.CurrentRevision, workflow.Context, now, workflow.CompletedAt, workflow.Version-1)
	if err != nil {
		return fmt.Errorf("update workflow: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrWorkflowConflict
	}
	return nil
}

func (r *WorkflowRepository) GetByTenantAndState(ctx context.Context, tenantID, state string, limit int) ([]*domain.WorkflowInstance, error) {
	if limit <= 0 {
		limit = 100
	}
	query := `
		SELECT workflow_id, tenant_id, workflow_type, state, version, current_revision, context, created_at, updated_at, completed_at
		FROM workflow_instances
		WHERE tenant_id = $1`
	args := []any{tenantID}
	if state != "" {
		query += " AND state = $2"
		args = append(args, state)
	}
	query += fmt.Sprintf(" ORDER BY updated_at LIMIT $%d", len(args)+1)
	args = append(args, limit)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get workflows by tenant and state: %w", err)
	}
	defer rows.Close()

	var workflows []*domain.WorkflowInstance
	for rows.Next() {
		w := &domain.WorkflowInstance{}
		var completedAt *time.Time
		if err := rows.Scan(
			&w.WorkflowID, &w.TenantID, &w.WorkflowType, &w.State, &w.Version,
			&w.CurrentRevision, &w.Context, &w.CreatedAt, &w.UpdatedAt, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workflow: %w", err)
		}
		w.CompletedAt = completedAt
		workflows = append(workflows, w)
	}
	return workflows, nil
}

func (r *WorkflowRepository) GetByCorrelation(ctx context.Context, correlationID string) ([]*domain.WorkflowInstance, error) {
	rows, err := r.db.Query(ctx, `
		SELECT workflow_id, tenant_id, workflow_type, state, version, current_revision, context, created_at, updated_at, completed_at
		FROM workflow_instances
		WHERE (context->>'correlation_id') = $1
		ORDER BY updated_at
	`, correlationID)
	if err != nil {
		return nil, fmt.Errorf("get workflows by correlation: %w", err)
	}
	defer rows.Close()

	var workflows []*domain.WorkflowInstance
	for rows.Next() {
		w := &domain.WorkflowInstance{}
		var completedAt *time.Time
		if err := rows.Scan(
			&w.WorkflowID, &w.TenantID, &w.WorkflowType, &w.State, &w.Version,
			&w.CurrentRevision, &w.Context, &w.CreatedAt, &w.UpdatedAt, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workflow: %w", err)
		}
		w.CompletedAt = completedAt
		workflows = append(workflows, w)
	}
	return workflows, nil
}

type WorkflowDefinitionRepository struct {
	db Querier
}

func NewWorkflowDefinitionRepository(db Querier) *WorkflowDefinitionRepository {
	return &WorkflowDefinitionRepository{db: db}
}

func (r *WorkflowDefinitionRepository) Save(ctx context.Context, def *domain.WorkflowDefinition) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO workflow_definitions (workflow_type, version, states, transitions, rules, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (workflow_type, version) DO UPDATE SET
			states = EXCLUDED.states,
			transitions = EXCLUDED.transitions,
			rules = EXCLUDED.rules,
			updated_at = EXCLUDED.updated_at
	`, def.WorkflowType, def.Version, def.States, def.Transitions, def.Rules, def.CreatedAt, def.UpdatedAt)
	if err != nil {
		return fmt.Errorf("save workflow definition: %w", err)
	}
	return nil
}

func (r *WorkflowDefinitionRepository) Get(ctx context.Context, workflowType string, version int64) (*domain.WorkflowDefinition, error) {
	def := &domain.WorkflowDefinition{}
	err := r.db.QueryRow(ctx, `
		SELECT workflow_type, version, states, transitions, rules, created_at, updated_at
		FROM workflow_definitions
		WHERE workflow_type = $1 AND version = $2
	`, workflowType, version).Scan(
		&def.WorkflowType, &def.Version, &def.States, &def.Transitions, &def.Rules, &def.CreatedAt, &def.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get workflow definition: %w", err)
	}
	return def, nil
}

func (r *WorkflowDefinitionRepository) List(ctx context.Context, tenantID string) ([]*domain.WorkflowDefinition, error) {
	rows, err := r.db.Query(ctx, `
		SELECT workflow_type, version, states, transitions, rules, created_at, updated_at
		FROM workflow_definitions
		ORDER BY workflow_type, version DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("list workflow definitions: %w", err)
	}
	defer rows.Close()

	var defs []*domain.WorkflowDefinition
	for rows.Next() {
		def := &domain.WorkflowDefinition{}
		if err := rows.Scan(
			&def.WorkflowType, &def.Version, &def.States, &def.Transitions, &def.Rules, &def.CreatedAt, &def.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workflow definition: %w", err)
		}
		defs = append(defs, def)
	}
	return defs, nil
}

func (r *WorkflowDefinitionRepository) Delete(ctx context.Context, workflowType string, version int64) error {
	_, err := r.db.Exec(ctx, `
		DELETE FROM workflow_definitions WHERE workflow_type = $1 AND version = $2
	`, workflowType, version)
	if err != nil {
		return fmt.Errorf("delete workflow definition: %w", err)
	}
	return nil
}
