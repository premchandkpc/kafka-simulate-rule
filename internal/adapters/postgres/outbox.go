package postgres

import (
	"context"
	"fmt"
	"strings"
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
		if ef.TenantID == "" {
			return fmt.Errorf("insert outbox: tenant_id is required")
		}
		_, err := r.db.Exec(ctx, `
			INSERT INTO outbox_effects (effect_id, execution_id, tenant_id, destination, name, payload, effect_type, status, attempts, max_attempts, available_at, last_error, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
			ON CONFLICT (effect_id) DO NOTHING
		`, ef.ID, ef.ExecutionID, ef.TenantID, ef.Destination, ef.Name, ef.Payload, ef.EffectType,
			ef.Status, ef.Attempts, ef.MaxAttempts, ef.AvailableAt, ef.LastError, ef.CreatedAt, ef.UpdatedAt)
		if err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}
	}
	return nil
}

func (r *OutboxRepository) ClaimPending(ctx context.Context, batchSize int, owner string, claimTTL time.Duration) ([]domain.OutboxEffect, error) {
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
			claim_expires_at = NOW() + $3,
			updated_at = NOW()
		FROM candidates
		WHERE o.effect_id = candidates.effect_id
		RETURNING o.effect_id, o.execution_id, o.tenant_id, o.destination, o.name, o.payload, o.effect_type,
		          o.status, o.attempts, o.max_attempts, o.available_at, o.last_error, o.created_at, o.updated_at
	`, batchSize, owner, claimTTL)
	if err != nil {
		return nil, fmt.Errorf("claim pending: %w", err)
	}
	defer rows.Close()

	var effects []domain.OutboxEffect
	for rows.Next() {
		var ef domain.OutboxEffect
		if err := rows.Scan(
			&ef.ID, &ef.ExecutionID, &ef.TenantID, &ef.Destination, &ef.Name, &ef.Payload, &ef.EffectType,
			&ef.Status, &ef.Attempts, &ef.MaxAttempts, &ef.AvailableAt, &ef.LastError, &ef.CreatedAt, &ef.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		effects = append(effects, ef)
	}
	return effects, nil
}

func (r *OutboxRepository) MarkDelivered(ctx context.Context, effectIDs []string, claimant string) error {
	if len(effectIDs) == 0 {
		return nil
	}
	now := time.Now().UTC()

	// Build placeholders for IN clause
	placeholders := make([]string, len(effectIDs))
	args := make([]interface{}, len(effectIDs)+2)
	args[0] = now
	args[1] = claimant
	for i, id := range effectIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+3)
		args[i+2] = id
	}

	query := fmt.Sprintf(`
		UPDATE outbox_effects
		SET status = 'delivered', claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL, updated_at = $1
		WHERE effect_id IN (%s) AND status = 'claimed' AND claimed_by = $2 AND claim_expires_at > NOW()
	`, strings.Join(placeholders, ","))

	tag, err := r.db.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("mark delivered: %w", err)
	}
	if int(tag.RowsAffected()) != len(effectIDs) {
		return domain.ErrOutboxClaimLost
	}
	return nil
}

func (r *OutboxRepository) ScheduleRetry(ctx context.Context, effectID, claimant string, delay time.Duration, attempts int, errMsg string) error {
	now := time.Now().UTC()
	availableAt := now.Add(delay)
	tag, err := r.db.Exec(ctx, `
		UPDATE outbox_effects
		SET status = 'pending', attempts = $3, available_at = $4, last_error = $5,
			claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL, updated_at = $6
		WHERE effect_id = $1 AND status = 'claimed' AND claimed_by = $2 AND claim_expires_at > NOW()
	`, effectID, claimant, attempts, availableAt, errMsg, now)
	if err != nil {
		return fmt.Errorf("schedule retry: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrOutboxClaimLost
	}
	return nil
}

func (r *OutboxRepository) Quarantine(ctx context.Context, effectID, claimant, errMsg string) error {
	now := time.Now().UTC()
	tag, err := r.db.Exec(ctx, `
		UPDATE outbox_effects
		SET status = 'quarantined', last_error = $3,
			claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL, updated_at = $4
		WHERE effect_id = $1 AND status = 'claimed' AND claimed_by = $2 AND claim_expires_at > NOW()
	`, effectID, claimant, errMsg, now)
	if err != nil {
		return fmt.Errorf("quarantine effect: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrOutboxClaimLost
	}
	return nil
}

func (r *OutboxRepository) GetPending(ctx context.Context, limit int) ([]domain.OutboxEffect, error) {
	rows, err := r.db.Query(ctx, `
		SELECT effect_id, execution_id, tenant_id, destination, name, payload, effect_type,
		       status, attempts, max_attempts, available_at, last_error, created_at, updated_at
		FROM outbox_effects
		WHERE status = 'pending' AND available_at <= NOW()
		ORDER BY available_at, created_at
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("get pending: %w", err)
	}
	defer rows.Close()

	var effects []domain.OutboxEffect
	for rows.Next() {
		var ef domain.OutboxEffect
		if err := rows.Scan(
			&ef.ID, &ef.ExecutionID, &ef.TenantID, &ef.Destination, &ef.Name, &ef.Payload, &ef.EffectType,
			&ef.Status, &ef.Attempts, &ef.MaxAttempts, &ef.AvailableAt, &ef.LastError, &ef.CreatedAt, &ef.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		effects = append(effects, ef)
	}
	return effects, nil
}

func (r *OutboxRepository) GetByExecution(ctx context.Context, executionID string) ([]domain.OutboxEffect, error) {
	rows, err := r.db.Query(ctx, `
		SELECT effect_id, execution_id, tenant_id, destination, name, payload, effect_type,
		       status, attempts, max_attempts, available_at, last_error, created_at, updated_at
		FROM outbox_effects
		WHERE execution_id = $1
		ORDER BY created_at
	`, executionID)
	if err != nil {
		return nil, fmt.Errorf("get by execution: %w", err)
	}
	defer rows.Close()

	var effects []domain.OutboxEffect
	for rows.Next() {
		var ef domain.OutboxEffect
		if err := rows.Scan(
			&ef.ID, &ef.ExecutionID, &ef.TenantID, &ef.Destination, &ef.Name, &ef.Payload, &ef.EffectType,
			&ef.Status, &ef.Attempts, &ef.MaxAttempts, &ef.AvailableAt, &ef.LastError, &ef.CreatedAt, &ef.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		effects = append(effects, ef)
	}
	return effects, nil
}

type ShardLeaseRepository struct {
	db Querier
}

func NewShardLeaseRepository(db Querier) *ShardLeaseRepository {
	return &ShardLeaseRepository{db: db}
}

func (r *ShardLeaseRepository) Acquire(ctx context.Context, shard uint32, owner string, ttl time.Duration) (*domain.ShardLease, error) {
	_, err := r.db.Exec(ctx, `
		INSERT INTO shard_leases (virtual_shard, owner, fencing_token, expires_at, routing_epoch)
		VALUES ($1, $2, 1, NOW() + ($3 * INTERVAL '1 millisecond'), 0)
		ON CONFLICT (virtual_shard) DO UPDATE SET
			owner = CASE WHEN shard_leases.expires_at < NOW() THEN EXCLUDED.owner ELSE shard_leases.owner END,
			fencing_token = CASE WHEN shard_leases.expires_at < NOW() THEN shard_leases.fencing_token + 1 ELSE shard_leases.fencing_token END,
			expires_at = CASE WHEN shard_leases.expires_at < NOW() THEN EXCLUDED.expires_at ELSE shard_leases.expires_at END
	`, shard, owner, ttl.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("acquire lease: %w", err)
	}
	return r.GetOwner(ctx, shard)
}

func (r *ShardLeaseRepository) Renew(ctx context.Context, shard uint32, owner string, fencingToken int64, ttl time.Duration) (*domain.ShardLease, error) {
	tag, err := r.db.Exec(ctx, `
		UPDATE shard_leases
		SET expires_at = NOW() + ($4 * INTERVAL '1 millisecond')
		WHERE virtual_shard = $1 AND owner = $2 AND fencing_token = $3 AND expires_at > NOW()
	`, shard, owner, fencingToken, ttl.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("renew lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, domain.ErrLeaseExpired
	}
	return r.GetOwner(ctx, shard)
}

// RenewOwned renews the supplied leases in one database round trip. The expected
// fencing token is part of each row match so a stale local snapshot cannot renew
// a lease that has already been reassigned. Rows omitted from the result are lost.
func (r *ShardLeaseRepository) RenewOwned(ctx context.Context, owner string, leases map[uint32]int64, ttl time.Duration) (map[uint32]*domain.ShardLease, error) {
	renewed := make(map[uint32]*domain.ShardLease, len(leases))
	if len(leases) == 0 {
		return renewed, nil
	}

	args := make([]any, 0, 2+2*len(leases))
	args = append(args, owner, ttl.Milliseconds())
	values := make([]string, 0, len(leases))
	for shardID, token := range leases {
		base := len(args) + 1
		values = append(values, fmt.Sprintf("($%d::bigint, $%d::bigint)", base, base+1))
		args = append(args, int64(shardID), token)
	}

	query := fmt.Sprintf(`
		UPDATE shard_leases AS lease
		SET expires_at = NOW() + ($2 * INTERVAL '1 millisecond')
		FROM (VALUES %s) AS requested(virtual_shard, fencing_token)
		WHERE lease.virtual_shard = requested.virtual_shard
		  AND lease.fencing_token = requested.fencing_token
		  AND lease.owner = $1
		  AND lease.expires_at > NOW()
		RETURNING lease.virtual_shard, lease.owner, lease.fencing_token, lease.expires_at, lease.routing_epoch
	`, strings.Join(values, ","))
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("renew owned leases: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		lease := &domain.ShardLease{}
		if err := rows.Scan(&lease.VirtualShard, &lease.Owner, &lease.FencingToken, &lease.ExpiresAt, &lease.RoutingEpoch); err != nil {
			return nil, fmt.Errorf("scan renewed lease: %w", err)
		}
		renewed[lease.VirtualShard] = lease
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate renewed leases: %w", err)
	}
	return renewed, nil
}
func (r *ShardLeaseRepository) Release(ctx context.Context, shard uint32, owner string) error {
	// Preserve the row so the fencing token remains monotonic across release/reacquire.
	// Deleting it would let the next INSERT reset the token to 1 and revive a zombie.
	_, err := r.db.Exec(ctx, `
		UPDATE shard_leases SET expires_at = NOW() - INTERVAL '1 millisecond'
		WHERE virtual_shard = $1 AND owner = $2
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

func (r *ShardLeaseRepository) ValidateFencingToken(ctx context.Context, shard uint32, owner string, fencingToken int64) error {
	var currentToken int64
	err := r.db.QueryRow(ctx, `
		SELECT fencing_token FROM shard_leases
		WHERE virtual_shard = $1 AND owner = $2 AND expires_at > NOW()
	`, shard, owner).Scan(&currentToken)
	if err != nil {
		return fmt.Errorf("validate fencing token: %w", err)
	}
	if currentToken != fencingToken {
		return domain.ErrFencingTokenMismatch
	}
	return nil
}

// AssertHeld validates the lease is held by the owner with the given fencing token.
// Uses row lock (FOR SHARE) to prevent stale holders from proceeding. Fails closed.
func (r *ShardLeaseRepository) AssertHeld(ctx context.Context, shard uint32, owner string, fencingToken int64) error {
	var currentToken int64
	var expiresAt time.Time
	var active bool
	err := r.db.QueryRow(ctx, `
		SELECT fencing_token, expires_at, expires_at > NOW()
		FROM shard_leases
		WHERE virtual_shard = $1 AND owner = $2
		FOR SHARE
	`, shard, owner).Scan(&currentToken, &expiresAt, &active)
	if err != nil {
		return fmt.Errorf("assert held: %w", err)
	}
	// Fail closed: token <= 0 is invalid, lease must not be expired
	if fencingToken <= 0 {
		return domain.ErrFencingTokenMismatch
	}
	if !active {
		return domain.ErrLeaseExpired
	}
	if currentToken != fencingToken {
		return domain.ErrFencingTokenMismatch
	}
	return nil
}

func (r *ShardLeaseRepository) ListOwned(ctx context.Context, owner string) ([]*domain.ShardLease, error) {
	rows, err := r.db.Query(ctx, `
		SELECT virtual_shard, owner, fencing_token, expires_at, routing_epoch
		FROM shard_leases
		WHERE owner = $1
	`, owner)
	if err != nil {
		return nil, fmt.Errorf("list owned leases: %w", err)
	}
	defer rows.Close()

	var leases []*domain.ShardLease
	for rows.Next() {
		lease := &domain.ShardLease{}
		if err := rows.Scan(
			&lease.VirtualShard, &lease.Owner, &lease.FencingToken, &lease.ExpiresAt, &lease.RoutingEpoch,
		); err != nil {
			return nil, fmt.Errorf("scan lease: %w", err)
		}
		leases = append(leases, lease)
	}
	return leases, nil
}
