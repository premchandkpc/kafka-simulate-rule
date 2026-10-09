package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/jackc/pgx/v5"
)

// QuarantineRepository persists failure evidence. Replay records an operator
// action; source-specific republishing belongs to a dedicated replay workflow.
type QuarantineRepository struct {
	db Querier
}

func NewQuarantineRepository(db Querier) *QuarantineRepository {
	return &QuarantineRepository{db: db}
}

func (r *QuarantineRepository) Save(ctx context.Context, entry *domain.QuarantineEntry) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO quarantine (quarantine_id, source_type, source_id, error_class, payload_ref, event_id, tenant_id, error_message, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
	`, entry.ID, entry.SourceType, entry.SourceID, entry.ErrorClass, entry.PayloadRef, entry.EventID, entry.TenantID, entry.Error, entry.CreatedAt)
	if err != nil {
		return fmt.Errorf("save quarantine entry: %w", err)
	}
	return nil
}

func (r *QuarantineRepository) Get(ctx context.Context, id string) (*domain.QuarantineEntry, error) {
	entry := &domain.QuarantineEntry{}
	err := r.db.QueryRow(ctx, `
		SELECT quarantine_id, source_type, source_id, error_class, COALESCE(payload_ref, ''),
		       COALESCE(event_id, ''), COALESCE(tenant_id, ''), COALESCE(error_message, ''), created_at
		FROM quarantine WHERE quarantine_id = $1
	`, id).Scan(&entry.ID, &entry.SourceType, &entry.SourceID, &entry.ErrorClass, &entry.PayloadRef,
		&entry.EventID, &entry.TenantID, &entry.Error, &entry.CreatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get quarantine entry: %w", err)
	}
	return entry, nil
}

func (r *QuarantineRepository) Replay(ctx context.Context, id string) error {
	tag, err := r.db.Exec(ctx, `UPDATE quarantine SET updated_at = NOW() WHERE quarantine_id = $1`, id)
	if err != nil {
		return fmt.Errorf("record quarantine replay: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrQuarantineNotFound
	}
	return nil
}

func (r *QuarantineRepository) Delete(ctx context.Context, id string) error {
	_, err := r.db.Exec(ctx, `DELETE FROM quarantine WHERE quarantine_id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete quarantine entry: %w", err)
	}
	return nil
}

func (r *QuarantineRepository) List(ctx context.Context, filter ports.QuarantineFilter) ([]*domain.QuarantineEntry, error) {
	if filter.Limit <= 0 {
		filter.Limit = 100
	}

	query := `
		SELECT quarantine_id, source_type, source_id, error_class, COALESCE(payload_ref, ''),
		       COALESCE(event_id, ''), COALESCE(tenant_id, ''), COALESCE(error_message, ''), created_at
		FROM quarantine WHERE 1=1
	`
	args := []interface{}{}
	argIdx := 1

	if filter.TenantID != "" {
		query += fmt.Sprintf(" AND tenant_id = $%d", argIdx)
		args = append(args, filter.TenantID)
		argIdx++
	}
	if filter.SourceType != "" {
		query += fmt.Sprintf(" AND source_type = $%d", argIdx)
		args = append(args, filter.SourceType)
		argIdx++
	}
	if filter.ErrorClass != "" {
		query += fmt.Sprintf(" AND error_class = $%d", argIdx)
		args = append(args, filter.ErrorClass)
		argIdx++
	}
	if !filter.From.IsZero() {
		query += fmt.Sprintf(" AND created_at >= $%d", argIdx)
		args = append(args, filter.From)
		argIdx++
	}
	if !filter.To.IsZero() {
		query += fmt.Sprintf(" AND created_at <= $%d", argIdx)
		args = append(args, filter.To)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
	args = append(args, filter.Limit, filter.Offset)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list quarantine entries: %w", err)
	}
	defer rows.Close()

	var entries []*domain.QuarantineEntry
	for rows.Next() {
		entry := &domain.QuarantineEntry{}
		err := rows.Scan(&entry.ID, &entry.SourceType, &entry.SourceID, &entry.ErrorClass, &entry.PayloadRef,
			&entry.EventID, &entry.TenantID, &entry.Error, &entry.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("scan quarantine entry: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

type QuarantineFilter struct {
	TenantID   string
	SourceType string
	ErrorClass string
	From       time.Time
	To         time.Time
	Limit      int
	Offset     int
}
