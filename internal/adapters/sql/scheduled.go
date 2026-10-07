package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/jackc/pgx/v5"
)

type ScheduledEventRepository struct {
	db Querier
}

func NewScheduledEventRepository(db Querier) *ScheduledEventRepository {
	return &ScheduledEventRepository{db: db}
}

func (r *ScheduledEventRepository) Save(ctx context.Context, event *domain.ScheduledEvent) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO scheduled_events (event_id, tenant_id, event_type, partition_key, workflow_id, payload, headers, scheduled_at, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (event_id) DO NOTHING
	`, event.EventID, event.TenantID, event.EventType, event.PartitionKey, event.WorkflowID, event.Payload, event.Headers, event.ScheduledAt, event.Status, event.CreatedAt)
	if err != nil {
		return fmt.Errorf("save scheduled event: %w", err)
	}
	return nil
}

func (r *ScheduledEventRepository) GetDue(ctx context.Context, before time.Time, limit int) ([]*domain.ScheduledEvent, error) {
	rows, err := r.db.Query(ctx, `
		SELECT event_id, tenant_id, event_type, partition_key, workflow_id, payload, headers, scheduled_at, status, created_at, released_at
		FROM scheduled_events
		WHERE status = 'pending' AND scheduled_at <= $1
		ORDER BY scheduled_at
		LIMIT $2
	`, before, limit)
	if err != nil {
		return nil, fmt.Errorf("get due scheduled events: %w", err)
	}
	defer rows.Close()

	var events []*domain.ScheduledEvent
	for rows.Next() {
		var evt domain.ScheduledEvent
		var releasedAt *time.Time
		if err := rows.Scan(
			&evt.EventID, &evt.TenantID, &evt.EventType, &evt.PartitionKey,
			&evt.WorkflowID, &evt.Payload, &evt.Headers, &evt.ScheduledAt,
			&evt.Status, &evt.CreatedAt, &releasedAt,
		); err != nil {
			return nil, fmt.Errorf("scan scheduled event: %w", err)
		}
		evt.ReleasedAt = releasedAt
		events = append(events, &evt)
	}
	return events, nil
}

func (r *ScheduledEventRepository) MarkReleased(ctx context.Context, eventID string, releasedAt time.Time) error {
	_, err := r.db.Exec(ctx, `
		UPDATE scheduled_events
		SET status = 'released', released_at = $2
		WHERE event_id = $1
	`, eventID, releasedAt)
	if err != nil {
		return fmt.Errorf("mark scheduled event released: %w", err)
	}
	return nil
}

func (r *ScheduledEventRepository) MarkFailed(ctx context.Context, eventID string, errMsg string) error {
	_, err := r.db.Exec(ctx, `
		UPDATE scheduled_events
		SET status = 'failed'
		WHERE event_id = $1
	`, eventID)
	if err != nil {
		return fmt.Errorf("mark scheduled event failed: %w", err)
	}
	return nil
}

func (r *ScheduledEventRepository) Get(ctx context.Context, eventID string) (*domain.ScheduledEvent, error) {
	evt := &domain.ScheduledEvent{}
	var releasedAt *time.Time
	err := r.db.QueryRow(ctx, `
		SELECT event_id, tenant_id, event_type, partition_key, workflow_id, payload, headers, scheduled_at, status, created_at, released_at
		FROM scheduled_events
		WHERE event_id = $1
	`, eventID).Scan(
		&evt.EventID, &evt.TenantID, &evt.EventType, &evt.PartitionKey,
		&evt.WorkflowID, &evt.Payload, &evt.Headers, &evt.ScheduledAt,
		&evt.Status, &evt.CreatedAt, &releasedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get scheduled event: %w", err)
	}
	evt.ReleasedAt = releasedAt
	return evt, nil
}