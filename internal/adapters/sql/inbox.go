package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type InboxRepository struct {
	pool *pgxpool.Pool
}

func NewInboxRepository(pool *pgxpool.Pool) *InboxRepository {
	return &InboxRepository{pool: pool}
}

func (r *InboxRepository) Insert(ctx context.Context, entry *domain.InboxEntry) (bool, error) {
	tag, err := r.pool.Exec(ctx, `
		INSERT INTO inbox (tenant_id, event_id, status, first_seen_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (tenant_id, event_id) DO NOTHING
	`, entry.TenantID, entry.EventID, entry.Status, entry.FirstSeenAt)
	if err != nil {
		return false, fmt.Errorf("insert inbox: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

func (r *InboxRepository) Get(ctx context.Context, tenantID string, eventID string) (*domain.InboxEntry, error) {
	entry := &domain.InboxEntry{}
	err := r.pool.QueryRow(ctx, `
		SELECT tenant_id, event_id, status, execution_id, first_seen_at, committed_at
		FROM inbox
		WHERE tenant_id = $1 AND event_id = $2
	`, tenantID, eventID).Scan(
		&entry.TenantID, &entry.EventID, &entry.Status, &entry.ExecutionID,
		&entry.FirstSeenAt, &entry.CommittedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get inbox: %w", err)
	}
	return entry, nil
}

func (r *InboxRepository) MarkCommitted(ctx context.Context, tenantID string, eventID string, executionID string) error {
	now := time.Now().UTC()
	_, err := r.pool.Exec(ctx, `
		UPDATE inbox SET status = 'committed', execution_id = $3, committed_at = $4
		WHERE tenant_id = $1 AND event_id = $2
	`, tenantID, eventID, executionID, now)
	if err != nil {
		return fmt.Errorf("mark committed: %w", err)
	}
	return nil
}

type ExecutionRepository struct {
	pool *pgxpool.Pool
}

func NewExecutionRepository(pool *pgxpool.Pool) *ExecutionRepository {
	return &ExecutionRepository{pool: pool}
}

func (r *ExecutionRepository) Save(ctx context.Context, execution *domain.Execution) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO executions (execution_id, event_id, tenant_id, rule_set, revision, decision_hash, status, error, trace_id, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (execution_id) DO NOTHING
	`, execution.ID, execution.EventID, execution.TenantID, execution.RuleSet, execution.Revision,
		execution.DecisionHash, execution.Status, execution.Error, execution.TraceID, execution.CreatedAt)
	if err != nil {
		return fmt.Errorf("save execution: %w", err)
	}
	return nil
}

func (r *ExecutionRepository) Get(ctx context.Context, executionID string) (*domain.Execution, error) {
	exec := &domain.Execution{}
	err := r.pool.QueryRow(ctx, `
		SELECT execution_id, event_id, tenant_id, rule_set, revision, decision_hash, status, error, trace_id, created_at, completed_at
		FROM executions
		WHERE execution_id = $1
	`, executionID).Scan(
		&exec.ID, &exec.EventID, &exec.TenantID, &exec.RuleSet, &exec.Revision,
		&exec.DecisionHash, &exec.Status, &exec.Error, &exec.TraceID, &exec.CreatedAt, &exec.CompletedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get execution: %w", err)
	}
	return exec, nil
}

func (r *ExecutionRepository) UpdateStatus(ctx context.Context, executionID string, status domain.ExecutionStatus, errMsg string) error {
	var completedAt *time.Time
	if status == domain.ExecutionStatusCompleted || status == domain.ExecutionStatusFailed || status == domain.ExecutionStatusQuarantined {
		now := time.Now().UTC()
		completedAt = &now
	}
	_, err := r.pool.Exec(ctx, `
		UPDATE executions SET status = $2, error = $3, completed_at = $4
		WHERE execution_id = $1
	`, executionID, status, errMsg, completedAt)
	if err != nil {
		return fmt.Errorf("update execution status: %w", err)
	}
	return nil
}