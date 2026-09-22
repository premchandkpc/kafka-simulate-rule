package sql

import (
	"context"
	"fmt"

	"github.com/flowrule/flowrule/internal/domain"
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
