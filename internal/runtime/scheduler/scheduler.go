package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type Scheduler struct {
	repo         ports.ScheduledEventRepository
	publisher    ports.EventPublisher
	clock        ports.Clock
	pollInterval time.Duration
	batchSize    int
	numShards    uint32

	mu       sync.Mutex
	running  bool
	stopped  bool
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewScheduler(
	repo ports.ScheduledEventRepository,
	publisher ports.EventPublisher,
	clock ports.Clock,
	pollInterval time.Duration,
	batchSize int,
	numShards uint32,
) *Scheduler {
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	if numShards == 0 {
		numShards = 4096
	}
	return &Scheduler{
		repo:         repo,
		publisher:    publisher,
		clock:        clock,
		pollInterval: pollInterval,
		batchSize:    batchSize,
		numShards:    numShards,
		stopChan:     make(chan struct{}),
	}
}

// Start begins the scheduler once. Scheduler instances cannot be restarted after Stop.
func (s *Scheduler) Start(ctx context.Context) {
	if ctx == nil {
		log.Printf("scheduler: refusing to start with nil context")
		return
	}

	s.mu.Lock()
	if s.running || s.stopped {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.wg.Add(1)
	s.mu.Unlock()

	go s.runLoop(ctx)
	log.Println("scheduler started")
}

// Stop is idempotent and waits for the scheduler loop to exit.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		close(s.stopChan)
	}
	s.running = false
	s.mu.Unlock()

	s.wg.Wait()
	log.Println("scheduler stopped")
}

func (s *Scheduler) runLoop(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	// Initial tick
	s.tick(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) {
	now := s.clock.Now()

	events, err := s.repo.GetDue(ctx, now, s.batchSize)
	if err != nil {
		log.Printf("scheduler: get due events: %v", err)
		return
	}

	if len(events) == 0 {
		return
	}

	// Claim the events with a lease to prevent duplicate processing
	eventIDs := make([]string, len(events))
	for i, evt := range events {
		eventIDs[i] = evt.EventID
	}

	// Give each claim batch a unique token. A later tick must not be able to
	// settle work claimed by an earlier tick after that claim has expired.
	claimant := "scheduler-" + domain.NewID()
	claimedEvents, err := s.repo.Claim(ctx, eventIDs, claimant, 5*time.Minute)
	if err != nil {
		log.Printf("scheduler: claim events: %v", err)
		return
	}

	log.Printf("scheduler: releasing %d scheduled events", len(claimedEvents))

	for _, evt := range claimedEvents {
		// Use idempotency key based on event ID to prevent duplicate publication
		idempotencyKey := fmt.Sprintf("sched-%s", evt.EventID)
		if err := s.releaseEvent(ctx, evt, idempotencyKey); err != nil {
			log.Printf("scheduler: release event %s: %v", evt.EventID, err)
			// Mark as failed
			if markErr := s.repo.MarkFailed(ctx, evt.EventID, claimant, err.Error()); markErr != nil {
				log.Printf("scheduler: mark failed %s: %v", evt.EventID, markErr)
			}
			continue
		}

		// Mark released with the same timestamp used for idempotency
		if err := s.repo.MarkReleased(ctx, evt.EventID, claimant, now); err != nil {
			log.Printf("scheduler: mark released %s: %v", evt.EventID, err)
		}
	}
}

func (s *Scheduler) releaseEvent(ctx context.Context, evt *domain.ScheduledEvent, idempotencyKey string) error {
	env := &domain.EventEnvelope{
		ID:           evt.EventID,
		Type:         evt.EventType,
		TenantID:     evt.TenantID,
		PartitionKey: evt.PartitionKey,
		WorkflowID:   evt.WorkflowID,
		OccurredAt:   s.clock.Now(),
		Data:         evt.Payload,
		Headers:      evt.Headers,
	}

	// Add idempotency key to headers
	if env.Headers == nil {
		env.Headers = make(map[string]string)
	}
	env.Headers["Idempotency-Key"] = idempotencyKey

	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// Publish to shard-specific subject using the shared base subject
	shard := env.VirtualShard(s.numShards)
	baseSubject := "events" // Must match consumer stream subject pattern
	return s.publisher.PublishToShard(ctx, baseSubject, shard, data)
}
