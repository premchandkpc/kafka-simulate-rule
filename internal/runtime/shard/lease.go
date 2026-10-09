package shard

import (
	"context"
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
}

func NewLeaseManager(
	repo ports.ShardLeaseRepository,
	clock ports.Clock,
	owner string,
	numShards uint32,
	ttl time.Duration,
) *LeaseManager {
	if numShards == 0 {
		numShards = 4096
	}
	if ttl == 0 {
		ttl = 30 * time.Second
	}
	return &LeaseManager{
		repo:          repo,
		clock:         clock,
		owner:         owner,
		numShards:     numShards,
		ttl:           ttl,
		renewInterval: ttl / 3,
		ownedShards:   make(map[uint32]*domain.ShardLease),
		stopRenew:     make(chan struct{}),
	}
}

func (m *LeaseManager) Start(ctx context.Context) error {
	m.wg.Add(1)
	go m.renewLoop(ctx)

	shardsPerWorker := m.numShards / 3
	start := uint32(0)
	if m.owner != "" {
		h := fnv32a(m.owner)
		start = h % m.numShards
	}

	for i := uint32(0); i < shardsPerWorker; i++ {
		shard := (start + i) % m.numShards
		if err := m.acquireShard(ctx, shard); err != nil {
			log.Printf("shard %d: acquire failed: %v", shard, err)
		}
	}
	return nil
}

func (m *LeaseManager) Stop(ctx context.Context) {
	close(m.stopRenew)
	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()
	for shard := range m.ownedShards {
		if err := m.repo.Release(ctx, shard, m.owner); err != nil {
			log.Printf("shard %d: release failed: %v", shard, err)
		}
	}
	m.ownedShards = make(map[uint32]*domain.ShardLease)
}

func (m *LeaseManager) acquireShard(ctx context.Context, shard uint32) error {
	lease, err := m.repo.Acquire(ctx, shard, m.owner, m.ttl)
	if err != nil {
		return err
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
	shards := make([]uint32, 0, len(m.ownedShards))
	for shard, lease := range m.ownedShards {
		shards = append(shards, shard)
		_ = lease
	}
	m.mu.RUnlock()

	for _, shard := range shards {
		m.mu.RLock()
		lease := m.ownedShards[shard]
		m.mu.RUnlock()
		if lease == nil {
			continue
		}
		newLease, err := m.repo.Renew(ctx, shard, m.owner, lease.FencingToken, m.ttl)
		if err != nil {
			// Handle lease loss - remove from owned shards
			if err == domain.ErrLeaseExpired || err == domain.ErrFencingTokenMismatch || err == domain.ErrLeaseOwnedByOther {
				log.Printf("shard %d: lease lost (%v), removing from owned shards", shard, err)
				m.mu.Lock()
				delete(m.ownedShards, shard)
				m.mu.Unlock()
				// Don't immediately re-acquire - let the assignment algorithm handle it
			} else {
				// Transient error - log but keep the shard in owned map
				log.Printf("shard %d: renew failed (transient): %v", shard, err)
			}
			continue
		}
		m.mu.Lock()
		m.ownedShards[shard] = newLease
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
