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
	Status() WorkerStatus
}

type WorkerStatus struct {
	Running        bool
	ProcessedCount int64
	ErrorCount     int64
	StartTime      time.Time
	LastProcessed  time.Time
}

// KeyQueue manages per-partition-key ordering
type KeyQueue interface {
	Submit(ctx context.Context, item QueueItem) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Stats() QueueStats
}

type QueueItem struct {
	Event         *domain.EventEnvelope
	Delivery      Delivery
	FencingToken  int64
	Shard         uint32
	WorkerID      string
	Priority      int
	SubmittedAt   time.Time
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
	Cancel(ctx context.Context, eventID string) error
	Status() SchedulerStatus
}

type SchedulerStatus struct {
	Running         bool
	ScheduledCount  int64
	ReleasedCount   int64
	FailedCount     int64
	NextRun         time.Time
	PollInterval    time.Duration
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
	Status() ShardManagerStatus
}

type ShardManagerStatus struct {
	OwnedShards    []uint32
	TotalShards    uint32
	LeaseTTL       time.Duration
	RenewalInterval time.Duration
	LastRenewal    time.Time
}

// BatchProcessor processes accumulated events
type BatchProcessor interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Trigger(ctx context.Context, tenantID, partitionKey, ruleSet string) error
	Status() BatchProcessorStatus
}

type BatchProcessorStatus struct {
	Running         bool
	PendingBatches  int
	ProcessingCount int64
	CompletedCount  int64
	FailedCount     int64
}

// EffectDispatcher dispatches effects to their destinations
type EffectDispatcher interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Dispatch(ctx context.Context, effect *domain.Effect) error
	DispatchBatch(ctx context.Context, effects []domain.Effect) error
	Status() EffectDispatcherStatus
}

type EffectDispatcherStatus struct {
	Running        bool
	PendingCount   int64
	DispatchedCount int64
	FailedCount    int64
	QueueDepth     int
}