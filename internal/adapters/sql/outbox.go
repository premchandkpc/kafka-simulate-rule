package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
)

type OutboxRepository struct {
	db Querier
}

func NewOutboxRepository(db Querier) *OutboxRepository {
	return &OutboxRepository{db: db}
}

func (r *OutboxRepository) Insert(ctx context.Context, effects []domain.OutboxEffect) error {
	for _, ef := range effects {
		_, err := r.db.Exec(ctx, `
			INSERT INTO outbox_effects (effect_id, execution_id, destination, name, payload, effect_type, status, attempts, max_attempts, available_at, last_error, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (effect_id) DO NOTHING
		`, ef.ID, ef.ExecutionID, ef.Destination, ef.Name, ef.Payload, ef.EffectType,
			ef.Status, ef.Attempts, ef.MaxAttempts, ef.AvailableAt, ef.LastError, ef.CreatedAt, ef.UpdatedAt)
		if err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}
	}
	return nil
}

func (r *OutboxRepository) ClaimPending(ctx context.Context, batchSize int, owner string) ([]domain.OutboxEffect, error) {
	rows, err := r.db.Query(ctx, `
		WITH candidates AS (
			SELECT effect_id
			FROM outbox_effects
			WHERE (status = 'pending' AND available_at <= NOW())
			   OR (status = 'claimed' AND claim_expires_at <= NOW())
			ORDER BY available_at, created_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_effects AS o
		SET status = 'claimed',
			claimed_by = $2,
			claimed_at = NOW(),
			claim_expires_at = NOW() + INTERVAL '1 minute',
			updated_at = NOW()
		FROM candidates
		WHERE o.effect_id = candidates.effect_id
		RETURNING o.effect_id, o.execution_id, o.destination, o.name, o.payload, o.effect_type,
		          o.status, o.attempts, o.max_attempts, o.available_at, o.last_error, o.created_at, o.updated_at
	`, batchSize, owner)
	if err != nil {
		return nil, fmt.Errorf("claim pending: %w", err)
	}
	defer rows.Close()

	var effects []domain.OutboxEffect
	for rows.Next() {
		var ef domain.OutboxEffect
		if err := rows.Scan(
			&ef.ID, &ef.ExecutionID, &ef.Destination, &ef.Name, &ef.Payload, &ef.EffectType,
			&ef.Status, &ef.Attempts, &ef.MaxAttempts, &ef.AvailableAt, &ef.LastError, &ef.CreatedAt, &ef.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		effects = append(effects, ef)
	}
	return effects, nil
}

func (r *OutboxRepository) MarkDelivered(ctx context.Context, effectID string) error {
	now := time.Now().UTC()
	_, err := r.db.Exec(ctx, `
		UPDATE outbox_effects
		SET status = 'delivered', claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL, updated_at = $2
		WHERE effect_id = $1
	`, effectID, now)
	if err != nil {
		return fmt.Errorf("mark delivered: %w", err)
	}
	return nil
}

func (r *OutboxRepository) ScheduleRetry(ctx context.Context, effectID string, delay time.Duration, attempts int, errMsg string) error {
	now := time.Now().UTC()
	availableAt := now.Add(delay)
	_, err := r.db.Exec(ctx, `
		UPDATE outbox_effects
		SET status = 'pending', attempts = $2, available_at = $3, last_error = $4,
			claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL, updated_at = $5
		WHERE effect_id = $1
	`, effectID, attempts, availableAt, errMsg, now)
	if err != nil {
		return fmt.Errorf("schedule retry: %w", err)
	}
	return nil
}

func (r *OutboxRepository) Quarantine(ctx context.Context, effectID string, errMsg string) error {
	now := time.Now().UTC()
	_, err := r.db.Exec(ctx, `
		UPDATE outbox_effects
		SET status = 'quarantined', last_error = $2,
			claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL, updated_at = $3
		WHERE effect_id = $1
	`, effectID, errMsg, now)
	if err != nil {
		return fmt.Errorf("quarantine effect: %w", err)
	}
	return nil
}

type ShardLeaseRepository struct {
	db Querier
}

func NewShardLeaseRepository(db Querier) *ShardLeaseRepository {
	return &ShardLeaseRepository{db: db}
}

func (r *ShardLeaseRepository) Acquire(ctx context.Context, shard uint32, owner string, ttl time.Duration) (*domain.ShardLease, error) {
	now := time.Now().UTC()
	expiresAt := now.Add(ttl)
	_, err := r.db.Exec(ctx, `
		INSERT INTO shard_leases (virtual_shard, owner, fencing_token, expires_at, routing_epoch)
		VALUES ($1, $2, 1, $3, 0)
		ON CONFLICT (virtual_shard) DO UPDATE SET
			owner = CASE WHEN shard_leases.expires_at < NOW() THEN EXCLUDED.owner ELSE shard_leases.owner END,
			fencing_token = CASE WHEN shard_leases.expires_at < NOW() THEN shard_leases.fencing_token + 1 ELSE shard_leases.fencing_token END,
			expires_at = CASE WHEN shard_leases.expires_at < NOW() THEN EXCLUDED.expires_at ELSE shard_leases.expires_at END
	`, shard, owner, expiresAt)
	if err != nil {
		return nil, fmt.Errorf("acquire lease: %w", err)
	}
	return r.GetOwner(ctx, shard)
}

func (r *ShardLeaseRepository) Renew(ctx context.Context, shard uint32, owner string, fencingToken int64, ttl time.Duration) (*domain.ShardLease, error) {
	now := time.Now().UTC()
	expiresAt := now.Add(ttl)
	tag, err := r.db.Exec(ctx, `
		UPDATE shard_leases SET expires_at = $3
		WHERE virtual_shard = $1 AND owner = $2 AND fencing_token = $4
	`, shard, owner, expiresAt, fencingToken)
	if err != nil {
		return nil, fmt.Errorf("renew lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, domain.ErrLeaseExpired
	}
	return r.GetOwner(ctx, shard)
}

func (r *ShardLeaseRepository) Release(ctx context.Context, shard uint32, owner string) error {
	_, err := r.db.Exec(ctx, `
		DELETE FROM shard_leases WHERE virtual_shard = $1 AND owner = $2
	`, shard, owner)
	if err != nil {
		return fmt.Errorf("release lease: %w", err)
	}
	return nil
}

func (r *ShardLeaseRepository) GetOwner(ctx context.Context, shard uint32) (*domain.ShardLease, error) {
	lease := &domain.ShardLease{}
	err := r.db.QueryRow(ctx, `
		SELECT virtual_shard, owner, fencing_token, expires_at, routing_epoch
		FROM shard_leases
		WHERE virtual_shard = $1
	`, shard).Scan(
		&lease.VirtualShard, &lease.Owner, &lease.FencingToken, &lease.ExpiresAt, &lease.RoutingEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf("get owner: %w", err)
	}
	return lease, nil
}
