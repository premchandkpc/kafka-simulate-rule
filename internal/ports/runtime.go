package ports

import (
	"context"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
)

// Worker manages the event processing lifecycle
type Worker interface {
	Run(ctx context.Context) error
	Stop(ctx context.Context) error
}

// KeyQueue manages per-partition-key ordering
type KeyQueue interface {
	Submit(ctx context.Context, item QueueItem) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Stats() QueueStats
}

type QueueItem struct {
	Event        *domain.EventEnvelope
	Delivery     Delivery
	FencingToken int64
	Shard        uint32
	WorkerID     string
	Priority     int
	SubmittedAt  time.Time
}

type QueueStats struct {
	ActiveKeys     int
	TotalQueued    int64
	Processing     int
	HotKeys        []HotKeyInfo
	GlobalDepth    int
	MaxGlobalDepth int
}

type HotKeyInfo struct {
	PartitionKey string
	Depth        int
	Since        time.Time
}

// Scheduler manages time-based event execution
type Scheduler interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Schedule(ctx context.Context, event *domain.ScheduledEvent) error
}

// ShardManager manages shard ownership and leases
type ShardManager interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	OwnedShards() []uint32
	IsOwner(shard uint32) bool
	GetFencingToken(shard uint32) (int64, bool)
	AcquireShard(ctx context.Context, shard uint32) error
	ReleaseShard(ctx context.Context, shard uint32) error
}

// BatchProcessor processes accumulated events
type BatchProcessor interface {
	Tick(ctx context.Context) (int, error)
}

// EffectDispatcher dispatches effects to their destinations
type EffectDispatcher interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Dispatch(ctx context.Context, effect *domain.Effect) error
}
