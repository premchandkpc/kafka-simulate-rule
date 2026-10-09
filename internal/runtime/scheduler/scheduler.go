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
	publisher    ports.BrokerPublisher
	clock        ports.Clock
	pollInterval time.Duration
	batchSize    int
	numShards    uint32

	mu       sync.Mutex
	running  bool
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewScheduler(
	repo ports.ScheduledEventRepository,
	publisher ports.BrokerPublisher,
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

func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.mu.Unlock()

	s.wg.Add(1)
	go s.runLoop(ctx)
	log.Println("scheduler started")
}

func (s *Scheduler) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	s.mu.Unlock()

	close(s.stopChan)
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

	claimedEvents, err := s.repo.Claim(ctx, eventIDs, "scheduler", 5*time.Minute)
	if err != nil {
		log.Printf("scheduler: claim events: %v", err)
		return
	}

	log.Printf("scheduler: releasing %d scheduled events", len(claimedEvents))

	for _, evt := range claimedEvents {
		if err := s.releaseEvent(ctx, evt); err != nil {
			log.Printf("scheduler: release event %s: %v", evt.EventID, err)
			// Mark as failed
			if markErr := s.repo.MarkFailed(ctx, evt.EventID, err.Error()); markErr != nil {
				log.Printf("scheduler: mark failed %s: %v", evt.EventID, markErr)
			}
			continue
		}

		if err := s.repo.MarkReleased(ctx, evt.EventID, now); err != nil {
			log.Printf("scheduler: mark released %s: %v", evt.EventID, err)
		}
	}
}

func (s *Scheduler) releaseEvent(ctx context.Context, evt *domain.ScheduledEvent) error {
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

	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// Publish to shard-specific subject using the shared base subject
	shard := env.VirtualShard(s.numShards)
	baseSubject := "events" // Must match consumer stream subject pattern
	return s.publisher.PublishToShard(ctx, baseSubject, shard, data)
}
