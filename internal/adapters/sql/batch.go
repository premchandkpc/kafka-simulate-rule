package sql

import (
	"context"
	"fmt"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/jackc/pgx/v5"
)

type BatchRepository struct {
	db Querier
}

func NewBatchRepository(db Querier) *BatchRepository {
	return &BatchRepository{db: db}
}

func (r *BatchRepository) ListUnbatched(ctx context.Context, limit int) ([]*domain.InboxEntry, error) {
	if limit <= 0 {
		limit = 400
	}
	rows, err := r.db.Query(ctx, `
		SELECT tenant_id, event_id, status, execution_id, first_seen_at, committed_at,
		       COALESCE(partition_key, ''), COALESCE(rule_set, ''), payload, COALESCE(batch_id, '')
		FROM inbox
		WHERE status = 'committed' AND batch_id IS NULL
		ORDER BY first_seen_at, tenant_id, event_id
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list unbatched: %w", err)
	}
	defer rows.Close()

	var entries []*domain.InboxEntry
	for rows.Next() {
		var e domain.InboxEntry
		if err := rows.Scan(
			&e.TenantID, &e.EventID, &e.Status, &e.ExecutionID,
			&e.FirstSeenAt, &e.CommittedAt,
			&e.PartitionKey, &e.RuleSet, &e.Payload, &e.BatchID,
		); err != nil {
			return nil, fmt.Errorf("scan unbatched: %w", err)
		}
		entries = append(entries, &e)
	}
	return entries, nil
}

func (r *BatchRepository) MarkBatched(ctx context.Context, tenantID, eventID, batchID string) error {
	_, err := r.db.Exec(ctx, `
		UPDATE inbox SET batch_id = $3
		WHERE tenant_id = $1 AND event_id = $2 AND batch_id IS NULL
	`, tenantID, eventID, batchID)
	if err != nil {
		return fmt.Errorf("mark batched: %w", err)
	}
	return nil
}

func (r *BatchRepository) SaveRun(ctx context.Context, run *domain.BatchRun) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO batch_runs (batch_id, tenant_id, partition_key, rule_set, status, member_count,
		                        execution_id, decision_hash, window_from, window_to, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (batch_id) DO UPDATE SET
			status = EXCLUDED.status,
			member_count = EXCLUDED.member_count,
			execution_id = EXCLUDED.execution_id,
			decision_hash = EXCLUDED.decision_hash,
			window_from = EXCLUDED.window_from,
			window_to = EXCLUDED.window_to
	`, run.BatchID, run.TenantID, run.PartitionKey, run.RuleSet, run.Status,
		run.MemberCount, run.ExecutionID, run.DecisionHash,
		run.WindowFrom, run.WindowTo, run.CreatedAt)
	if err != nil {
		return fmt.Errorf("save batch run: %w", err)
	}
	return nil
}

func (r *BatchRepository) GetRun(ctx context.Context, batchID string) (*domain.BatchRun, error) {
	run := &domain.BatchRun{}
	err := r.db.QueryRow(ctx, `
		SELECT batch_id, tenant_id, partition_key, rule_set, status, member_count,
		       execution_id, decision_hash, window_from, window_to, created_at
		FROM batch_runs
		WHERE batch_id = $1
	`, batchID).Scan(
		&run.BatchID, &run.TenantID, &run.PartitionKey, &run.RuleSet, &run.Status,
		&run.MemberCount, &run.ExecutionID, &run.DecisionHash,
		&run.WindowFrom, &run.WindowTo, &run.CreatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get batch run: %w", err)
	}
	return run, nil
}