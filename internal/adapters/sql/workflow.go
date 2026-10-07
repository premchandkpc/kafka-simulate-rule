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
	rows, err := r.db.Query(ctx, `
		SELECT workflow_id, tenant_id, workflow_type, state, version, current_revision, context, created_at, updated_at, completed_at
		FROM workflow_instances
		WHERE tenant_id = $1 AND state = $2
		ORDER BY updated_at
		LIMIT $3
	`, tenantID, state, limit)
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