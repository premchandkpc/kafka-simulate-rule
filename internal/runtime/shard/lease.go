package shard

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type LeaseManager struct {
	repo          ports.ShardLeaseRepository
	clock         ports.Clock
	owner         string
	numShards     uint32
	ttl           time.Duration
	renewInterval time.Duration

	mu          sync.RWMutex
	ownedShards map[uint32]*domain.ShardLease
	stopRenew   chan struct{}
	wg          sync.WaitGroup

	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
}

func NewLeaseManager(
	repo ports.ShardLeaseRepository,
	clock ports.Clock,
	owner string,
	numShards uint32,
	ttl time.Duration,
) *LeaseManager {
	return NewLeaseManagerWithRenewInterval(repo, clock, owner, numShards, ttl, 0)
}

// NewLeaseManagerWithRenewInterval constructs a lease manager with an explicit renewal interval.
// A non-positive renewal interval defaults to one third of the lease TTL.
func NewLeaseManagerWithRenewInterval(
	repo ports.ShardLeaseRepository,
	clock ports.Clock,
	owner string,
	numShards uint32,
	ttl time.Duration,
	renewInterval time.Duration,
) *LeaseManager {
	if numShards == 0 {
		numShards = 4096
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	if renewInterval <= 0 {
		renewInterval = ttl / 3
	}
	if renewInterval <= 0 {
		renewInterval = ttl
	}
	return &LeaseManager{
		repo:          repo,
		clock:         clock,
		owner:         owner,
		numShards:     numShards,
		ttl:           ttl,
		renewInterval: renewInterval,
		ownedShards:   make(map[uint32]*domain.ShardLease),
		stopRenew:     make(chan struct{}),
	}
}

func (m *LeaseManager) Start(ctx context.Context) error {
	if ctx == nil {
		return errors.New("start lease manager: context is nil")
	}

	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.stopped {
		return errors.New("start lease manager: manager is stopped")
	}
	if m.started {
		return nil
	}
	m.started = true

	target := uint32(m.numShards / 3)
	if target == 0 {
		target = 1
	}
	start := uint32(0)
	if m.owner != "" {
		start = fnv32a(m.owner) % m.numShards
	}

	for i := uint32(0); i < target; i++ {
		shardID := (start + i) % m.numShards
		if err := m.acquireShard(ctx, shardID); err != nil {
			if ctx.Err() != nil {
				m.started = false
				return fmt.Errorf("acquire initial shard %d: %w", shardID, ctx.Err())
			}
			// A shard owned by another worker is expected in a shared deployment.
			log.Printf("shard %d: initial acquire skipped: %v", shardID, err)
		}
	}

	m.wg.Add(1)
	go m.renewLoop(ctx)
	return nil
}

// Stop is idempotent, waits for lease renewal to stop, and releases leases with
// a bounded context that remains usable during signal-driven shutdown.
func (m *LeaseManager) Stop(ctx context.Context) { //nolint:contextcheck // intentionally assign to parameter for fallback
	if ctx == nil {
		ctx = context.Background()
	}

	m.lifecycleMu.Lock()
	if !m.stopped {
		m.stopped = true
		close(m.stopRenew)
	}
	m.lifecycleMu.Unlock()

	m.wg.Wait()

	m.mu.Lock()
	owned := m.ownedShards
	m.ownedShards = make(map[uint32]*domain.ShardLease)
	m.mu.Unlock()

	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for shardID := range owned {
		if err := m.repo.Release(releaseCtx, shardID, m.owner); err != nil {
			log.Printf("shard %d: release failed: %v", shardID, err)
		}
	}
}

func (m *LeaseManager) acquireShard(ctx context.Context, shard uint32) error {
	lease, err := m.repo.Acquire(ctx, shard, m.owner, m.ttl)
	if err != nil {
		return err
	}
	if lease == nil {
		return errors.New("acquire shard: repository returned a nil lease")
	}
	if lease.Owner != m.owner {
		return domain.ErrLeaseOwnedByOther
	}
	m.mu.Lock()
	m.ownedShards[shard] = lease
	m.mu.Unlock()
	log.Printf("acquired shard %d (fencing_token=%d)", shard, lease.FencingToken)
	return nil
}

func (m *LeaseManager) renewLoop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.renewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopRenew:
			return
		case <-ticker.C:
			m.renewOwned(ctx)
		}
	}
}

func (m *LeaseManager) renewOwned(ctx context.Context) {
	m.mu.RLock()
	leases := make(map[uint32]int64, len(m.ownedShards))
	for shardID, lease := range m.ownedShards {
		if lease != nil {
			leases[shardID] = lease.FencingToken
		}
	}
	m.mu.RUnlock()

	for shardID, fencingToken := range leases {
		newLease, err := m.repo.Renew(ctx, shardID, m.owner, fencingToken, m.ttl)
		if err != nil {
			// Handle lease loss - remove from owned shards
			if errors.Is(err, domain.ErrLeaseExpired) || errors.Is(err, domain.ErrFencingTokenMismatch) || errors.Is(err, domain.ErrLeaseOwnedByOther) {
				log.Printf("shard %d: lease lost (%v), removing from owned shards", shardID, err)
				m.mu.Lock()
				delete(m.ownedShards, shardID)
				m.mu.Unlock()
				// Don't immediately re-acquire - let the assignment algorithm handle it
			} else {
				// Transient error - log but keep the shard in owned map
				log.Printf("shard %d: renew failed (transient): %v", shardID, err)
			}
			continue
		}
		if newLease == nil {
			log.Printf("shard %d: renew returned an empty lease; retaining current ownership snapshot", shardID)
			continue
		}
		m.mu.Lock()
		m.ownedShards[shardID] = newLease
		m.mu.Unlock()
	}

	// Attempt to acquire new shards if we have capacity
	m.rebalanceIfNeeded(ctx)
}

// rebalanceIfNeeded attempts to acquire more shards if we own fewer than our fair share.
func (m *LeaseManager) rebalanceIfNeeded(ctx context.Context) {
	m.mu.RLock()
	currentOwned := len(m.ownedShards)
	m.mu.RUnlock()

	// Target: numShards / 3 (same as initial assignment)
	targetOwned := int(m.numShards / 3)
	if targetOwned < 1 {
		targetOwned = 1
	}

	if currentOwned >= targetOwned {
		return
	}

	// Try to acquire shards up to our target
	needed := targetOwned - currentOwned
	for i := uint32(0); i < m.numShards && needed > 0; i++ {
		m.mu.RLock()
		_, alreadyOwned := m.ownedShards[i]
		m.mu.RUnlock()

		if alreadyOwned {
			continue
		}

		if err := m.acquireShard(ctx, i); err == nil {
			needed--
		}
	}
}

func (m *LeaseManager) OwnedShards() []uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	shards := make([]uint32, 0, len(m.ownedShards))
	for shard := range m.ownedShards {
		shards = append(shards, shard)
	}
	return shards
}

func (m *LeaseManager) IsOwner(shard uint32) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.ownedShards[shard]
	return ok
}

func (m *LeaseManager) GetFencingToken(shard uint32) (int64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	lease, ok := m.ownedShards[shard]
	if !ok {
		return 0, false
	}
	return lease.FencingToken, true
}

func (m *LeaseManager) VirtualShardForEvent(env *domain.EventEnvelope) uint32 {
	return env.VirtualShard(m.numShards)
}

func fnv32a(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
