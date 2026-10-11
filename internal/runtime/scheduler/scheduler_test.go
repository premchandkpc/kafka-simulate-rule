package scheduler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type schedulerTestClock struct{ now time.Time }

func (c schedulerTestClock) Now() time.Time { return c.now }

type schedulerTestRepo struct {
	event       *domain.ScheduledEvent
	claimant    string
	releasedBy  string
	failedBy    string
	failedMsg   string
	claimErr    error
	releasedErr error
	failedErr   error
}

func (r *schedulerTestRepo) Save(context.Context, *domain.ScheduledEvent) error { return nil }
func (r *schedulerTestRepo) GetDue(context.Context, time.Time, int) ([]*domain.ScheduledEvent, error) {
	if r.event == nil {
		return nil, nil
	}
	return []*domain.ScheduledEvent{r.event}, nil
}
func (r *schedulerTestRepo) Claim(_ context.Context, _ []string, claimant string, _ time.Duration) ([]*domain.ScheduledEvent, error) {
	r.claimant = claimant
	if r.claimErr != nil {
		return nil, r.claimErr
	}
	return []*domain.ScheduledEvent{r.event}, nil
}
func (r *schedulerTestRepo) MarkReleased(_ context.Context, _ string, claimant string, _ time.Time) error {
	r.releasedBy = claimant
	return r.releasedErr
}
func (r *schedulerTestRepo) MarkFailed(_ context.Context, _ string, claimant, errMsg string) error {
	r.failedBy, r.failedMsg = claimant, errMsg
	return r.failedErr
}
func (r *schedulerTestRepo) Get(context.Context, string, string) (*domain.ScheduledEvent, error) {
	return r.event, nil
}
func (r *schedulerTestRepo) List(context.Context, string, int, int) ([]*domain.ScheduledEvent, error) {
	return nil, nil
}
func (r *schedulerTestRepo) Cancel(context.Context, string, string) error { return nil }

type schedulerTestPublisher struct {
	publishErr error
	subject    string
	payload    []byte
}

func (p *schedulerTestPublisher) Publish(context.Context, string, []byte) error { return nil }
func (p *schedulerTestPublisher) PublishBatch(context.Context, []ports.PublishMessage) error {
	return nil
}
func (p *schedulerTestPublisher) PublishToShard(_ context.Context, base string, shard uint32, payload []byte) error {
	p.subject = fmt.Sprintf("%s.shard.%d", base, shard)
	p.payload = append([]byte(nil), payload...)
	return p.publishErr
}
func (p *schedulerTestPublisher) Close() error { return nil }

func TestTickSettlesWithTheUniqueClaimToken(t *testing.T) {
	now := time.Date(2026, time.October, 11, 10, 0, 0, 0, time.UTC)
	repo := &schedulerTestRepo{event: &domain.ScheduledEvent{
		EventID: "scheduled-1", TenantID: "tenant-a", EventType: "order.reminder",
		PartitionKey: "order-1", Payload: []byte(`{"reminder":true}`), ScheduledAt: now.Add(-time.Minute),
	}}
	publisher := &schedulerTestPublisher{}
	s := NewScheduler(repo, publisher, schedulerTestClock{now: now}, time.Second, 10, 64)

	s.tick(context.Background())

	if repo.claimant == "" || repo.claimant == "scheduler" {
		t.Fatalf("claimant = %q, want a unique per-tick token", repo.claimant)
	}
	if repo.releasedBy != repo.claimant {
		t.Fatalf("released by %q, claimed by %q", repo.releasedBy, repo.claimant)
	}
	if publisher.subject == "" || len(publisher.payload) == 0 {
		t.Fatalf("publisher not called correctly: subject=%q payload=%q", publisher.subject, publisher.payload)
	}
}

func TestTickFencesFailedSettlementWithTheClaimToken(t *testing.T) {
	now := time.Date(2026, time.October, 11, 10, 0, 0, 0, time.UTC)
	repo := &schedulerTestRepo{event: &domain.ScheduledEvent{
		EventID: "scheduled-2", TenantID: "tenant-a", EventType: "order.reminder",
		PartitionKey: "order-2", Payload: []byte(`{"reminder":true}`), ScheduledAt: now.Add(-time.Minute),
	}}
	publisher := &schedulerTestPublisher{publishErr: errors.New("broker unavailable")}
	s := NewScheduler(repo, publisher, schedulerTestClock{now: now}, time.Second, 10, 64)

	s.tick(context.Background())

	if repo.failedBy != repo.claimant {
		t.Fatalf("failed by %q, claimed by %q", repo.failedBy, repo.claimant)
	}
	if repo.failedMsg == "" {
		t.Fatal("expected the publish error to be persisted")
	}
}
