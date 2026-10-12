package batches

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
)

type memBatches struct {
	mu      sync.Mutex
	entries map[string]*domain.InboxEntry
	runs    map[string]*domain.BatchRun
}

func newMemBatches() *memBatches {
	return &memBatches{
		entries: make(map[string]*domain.InboxEntry),
		runs:    make(map[string]*domain.BatchRun),
	}
}

func (m *memBatches) ListUnbatched(ctx context.Context, limit int) ([]*domain.InboxEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*domain.InboxEntry
	for _, e := range m.entries {
		if e.Status == domain.InboxStatusCommitted && e.BatchID == "" {
			result = append(result, e)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *memBatches) ListUnbatchedByIDs(ctx context.Context, tenantID, partitionKey, ruleSet string, eventIDs []string) ([]*domain.InboxEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	wanted := make(map[string]struct{}, len(eventIDs))
	for _, id := range eventIDs {
		wanted[id] = struct{}{}
	}
	var result []*domain.InboxEntry
	for _, e := range m.entries {
		if e.TenantID != tenantID || e.PartitionKey != partitionKey || e.RuleSet != ruleSet ||
			e.Status != domain.InboxStatusCommitted || e.BatchID != "" {
			continue
		}
		if _, ok := wanted[e.EventID]; ok {
			result = append(result, e)
		}
	}
	return result, nil
}

func (m *memBatches) MarkBatched(ctx context.Context, tenantID, eventID, batchID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := tenantID + "|" + eventID
	if e, ok := m.entries[k]; ok {
		e.BatchID = batchID
	}
	return nil
}

func (m *memBatches) SaveRun(ctx context.Context, run *domain.BatchRun) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runs[run.BatchID] = run
	return nil
}

func (m *memBatches) GetRun(ctx context.Context, batchID string) (*domain.BatchRun, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.runs[batchID], nil
}

func (m *memBatches) GetRunForTenant(ctx context.Context, tenantID, batchID string) (*domain.BatchRun, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	run := m.runs[batchID]
	if run == nil || run.TenantID != tenantID {
		return nil, nil
	}
	return run, nil
}

func (m *memBatches) ListRuns(ctx context.Context, tenantID, ruleSet string, limit, offset int) ([]*domain.BatchRun, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*domain.BatchRun
	for _, run := range m.runs {
		if run.TenantID != tenantID || (ruleSet != "" && run.RuleSet != ruleSet) {
			continue
		}
		result = append(result, run)
	}
	if offset >= len(result) {
		return []*domain.BatchRun{}, nil
	}
	result = result[offset:]
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

type memEventProcessor struct {
	mu         sync.Mutex
	envelopes  []*domain.EventEnvelope
	executions map[string]*domain.Execution
}

func newMemEventProcessor() *memEventProcessor {
	return &memEventProcessor{
		executions: make(map[string]*domain.Execution),
	}
}

func (m *memEventProcessor) Process(ctx context.Context, envelope *domain.EventEnvelope, fencingToken int64, shard uint32, workerID string) (*domain.Execution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.envelopes = append(m.envelopes, envelope)
	exec := &domain.Execution{
		ID:           "exec-" + envelope.ID,
		EventID:      envelope.ID,
		TenantID:     envelope.TenantID,
		RuleSet:      envelope.Type,
		Revision:     1,
		DecisionHash: "hash-" + envelope.ID,
		Status:       domain.ExecutionStatusCompleted,
		CreatedAt:    time.Now().UTC(),
	}
	m.executions[exec.ID] = exec
	return exec, nil
}

func (m *memEventProcessor) QuarantineEvent(ctx context.Context, sourceID, eventID, tenantID string, errClass domain.ErrorClass, errMsg string) error {
	return nil
}

func TestBatchService_Accumulate(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	clock := domain.SystemClock{}

	cfg := domain.BatchConfig{
		Mode:     domain.BatchModeAccumulate,
		MaxBatch: 3,
		Window:   0,
	}

	svc := NewService(batches, events, clock, cfg)

	ctx := context.Background()
	now := time.Now().UTC()

	for i := 1; i <= 3; i++ {
		id := "evt-" + string(rune('0'+i))
		key := "tenant-1|" + id
		batches.entries[key] = &domain.InboxEntry{
			TenantID:     "tenant-1",
			EventID:      id,
			PartitionKey: "pk-1",
			RuleSet:      "order.created",
			Status:       domain.InboxStatusCommitted,
			FirstSeenAt:  now.Add(time.Duration(i) * time.Second),
			Payload:      []byte(`{"id": "` + id + `", "total": 100}`),
		}
	}

	formed, err := svc.Tick(ctx)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if formed != 1 {
		t.Errorf("expected 1 batch formed, got %d", formed)
	}

	if len(events.envelopes) != 1 {
		t.Fatalf("expected 1 envelope, got %d", len(events.envelopes))
	}
	env := events.envelopes[0]
	if env.Type != "order.created.batch" {
		t.Errorf("expected batch rule set, got %s", env.Type)
	}

	var payload domain.BatchData
	if err := json.Unmarshal(env.Data, &payload); err != nil {
		t.Fatalf("unmarshal batch data: %v", err)
	}
	if len(payload.Events) != 3 {
		t.Errorf("expected 3 batch events, got %d", len(payload.Events))
	}

	expectedBatchID := domain.ComputeBatchID("tenant-1", "pk-1", "order.created", []string{"evt-1", "evt-2", "evt-3"})
	if env.ID != expectedBatchID {
		t.Errorf("batch ID mismatch: got %s, want %s", env.ID, expectedBatchID)
	}

	for _, id := range []string{"evt-1", "evt-2", "evt-3"} {
		e := batches.entries["tenant-1|"+id]
		if e.BatchID != expectedBatchID {
			t.Errorf("event %s not marked batched: %s", id, e.BatchID)
		}
	}
}

func TestBatchService_AccumulateBelowThreshold(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	clock := domain.SystemClock{}

	cfg := domain.BatchConfig{
		Mode:     domain.BatchModeAccumulate,
		MaxBatch: 5,
		Window:   0,
	}

	svc := NewService(batches, events, clock, cfg)

	ctx := context.Background()
	now := time.Now().UTC()

	for i := 1; i <= 2; i++ {
		id := "evt-" + string(rune('0'+i))
		batches.entries[id] = &domain.InboxEntry{
			TenantID:     "tenant-1",
			EventID:      id,
			PartitionKey: "pk-1",
			RuleSet:      "order.created",
			Status:       domain.InboxStatusCommitted,
			FirstSeenAt:  now.Add(time.Duration(i) * time.Second),
			Payload:      []byte(`{"id": "` + id + `", "total": 100}`),
		}
	}

	formed, err := svc.Tick(ctx)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if formed != 0 {
		t.Errorf("expected 0 batches formed, got %d", formed)
	}
	if len(events.envelopes) != 0 {
		t.Errorf("expected 0 envelopes, got %d", len(events.envelopes))
	}
}

func TestBatchService_Window(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	clock := domain.SystemClock{}

	cfg := domain.BatchConfig{
		Mode:     domain.BatchModeWindow,
		MaxBatch: 10,
		Window:   5 * time.Second,
	}

	svc := NewService(batches, events, clock, cfg)

	ctx := context.Background()
	now := time.Now().UTC()

	id := "evt-1"
	batches.entries[id] = &domain.InboxEntry{
		TenantID:     "tenant-1",
		EventID:      id,
		PartitionKey: "pk-1",
		RuleSet:      "order.created",
		Status:       domain.InboxStatusCommitted,
		FirstSeenAt:  now.Add(-10 * time.Second),
		Payload:      []byte(`{"id": "` + id + `", "total": 100}`),
	}

	formed, err := svc.Tick(ctx)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if formed != 1 {
		t.Errorf("expected 1 batch formed, got %d", formed)
	}
	if len(events.envelopes) != 1 {
		t.Fatalf("expected 1 envelope, got %d", len(events.envelopes))
	}
}

func TestBatchService_WindowNotElapsed(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	clock := domain.SystemClock{}

	cfg := domain.BatchConfig{
		Mode:     domain.BatchModeWindow,
		MaxBatch: 10,
		Window:   5 * time.Second,
	}

	svc := NewService(batches, events, clock, cfg)

	ctx := context.Background()
	now := time.Now().UTC()

	id := "evt-1"
	batches.entries[id] = &domain.InboxEntry{
		TenantID:     "tenant-1",
		EventID:      id,
		PartitionKey: "pk-1",
		RuleSet:      "order.created",
		Status:       domain.InboxStatusCommitted,
		FirstSeenAt:  now.Add(-1 * time.Second),
		Payload:      []byte(`{"id": "` + id + `", "total": 100}`),
	}

	formed, err := svc.Tick(ctx)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if formed != 0 {
		t.Errorf("expected 0 batches formed, got %d", formed)
	}
}

func TestBatchService_NoneMode(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	clock := domain.SystemClock{}

	cfg := domain.BatchConfig{
		Mode:     domain.BatchModeNone,
		MaxBatch: 10,
		Window:   5 * time.Second,
	}

	svc := NewService(batches, events, clock, cfg)

	ctx := context.Background()
	now := time.Now().UTC()

	id := "evt-1"
	batches.entries[id] = &domain.InboxEntry{
		TenantID:     "tenant-1",
		EventID:      id,
		PartitionKey: "pk-1",
		RuleSet:      "order.created",
		Status:       domain.InboxStatusCommitted,
		FirstSeenAt:  now.Add(-10 * time.Second),
		Payload:      []byte(`{"id": "` + id + `", "total": 100}`),
	}

	formed, err := svc.Tick(ctx)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if formed != 0 {
		t.Errorf("expected 0 batches formed, got %d", formed)
	}
}

func TestComputeBatchID_Deterministic(t *testing.T) {
	eventIDs := []string{"evt-3", "evt-1", "evt-2"}
	id1 := domain.ComputeBatchID("tenant-1", "pk-1", "order.created", eventIDs)

	eventIDs2 := []string{"evt-1", "evt-2", "evt-3"}
	id2 := domain.ComputeBatchID("tenant-1", "pk-1", "order.created", eventIDs2)

	if id1 != id2 {
		t.Errorf("batch ID not deterministic: %s != %s", id1, id2)
	}
}

func TestComputeBatchID_DifferentPartitions(t *testing.T) {
	eventIDs := []string{"evt-1", "evt-2"}
	id1 := domain.ComputeBatchID("tenant-1", "pk-1", "order.created", eventIDs)
	id2 := domain.ComputeBatchID("tenant-1", "pk-2", "order.created", eventIDs)

	if id1 == id2 {
		t.Error("batch ID should differ for different partition keys")
	}
}

func TestBatchService_ListBatchesScopesTenantAndRuleSet(t *testing.T) {
	batches := newMemBatches()
	batches.runs["batch-a"] = &domain.BatchRun{BatchID: "batch-a", TenantID: "tenant-a", RuleSet: "orders", CreatedAt: time.Now()}
	batches.runs["batch-b"] = &domain.BatchRun{BatchID: "batch-b", TenantID: "tenant-b", RuleSet: "orders", CreatedAt: time.Now()}
	batches.runs["batch-c"] = &domain.BatchRun{BatchID: "batch-c", TenantID: "tenant-a", RuleSet: "payments", CreatedAt: time.Now()}

	svc := NewService(batches, newMemEventProcessor(), domain.SystemClock{}, domain.BatchConfig{Mode: domain.BatchModeNone})
	got, err := svc.ListBatches(context.Background(), "tenant-a", "orders", 10, 0)
	if err != nil {
		t.Fatalf("ListBatches() error = %v", err)
	}
	if len(got) != 1 || got[0].BatchID != "batch-a" {
		t.Fatalf("ListBatches() = %#v, want only tenant-a/orders batch", got)
	}
}

func TestBatchService_GetBatchScopesTenant(t *testing.T) {
	batches := newMemBatches()
	batches.runs["batch-a"] = &domain.BatchRun{BatchID: "batch-a", TenantID: "tenant-a"}

	svc := NewService(batches, newMemEventProcessor(), domain.SystemClock{}, domain.BatchConfig{Mode: domain.BatchModeNone})
	got, err := svc.GetBatch(context.Background(), "tenant-b", "batch-a")
	if err != nil {
		t.Fatalf("GetBatch() error = %v", err)
	}
	if got != nil {
		t.Fatalf("GetBatch() = %#v, want nil for another tenant", got)
	}
}

func TestBatchService_CreateBatchFailsWhenProcessorIsNotConfigured(t *testing.T) {
	svc := NewService(newMemBatches(), nil, domain.SystemClock{}, domain.BatchConfig{Mode: domain.BatchModeNone})
	_, err := svc.CreateBatch(context.Background(), "tenant-a", "partition", "orders", []string{"event-1"})
	if !errors.Is(err, domain.ErrBatchProcessingUnavailable) {
		t.Fatalf("CreateBatch() error = %v, want ErrBatchProcessingUnavailable", err)
	}
}


func TestCreateBatchRequiresEveryRequestedEvent(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	now := time.Now().UTC()
	batches.entries["tenant-1|evt-1"] = &domain.InboxEntry{
		TenantID: "tenant-1", EventID: "evt-1", PartitionKey: "pk-1",
		RuleSet: "order.created", Status: domain.InboxStatusCommitted,
		FirstSeenAt: now, Payload: []byte(`{"id":"evt-1"}`),
	}
	service := NewService(batches, events, domain.SystemClock{}, domain.BatchConfig{MaxBatch: 10})

	_, err := service.CreateBatch(context.Background(), "tenant-1", "pk-1", "order.created", []string{"evt-1", "evt-missing"})
	if err == nil {
		t.Fatal("CreateBatch accepted a partial set of requested events")
	}
	if len(events.envelopes) != 0 {
		t.Fatalf("processed %d batch envelopes before validating membership", len(events.envelopes))
	}
}

func TestCreateBatchRejectsDuplicateEventIDs(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	service := NewService(batches, events, domain.SystemClock{}, domain.BatchConfig{MaxBatch: 10})

	_, err := service.CreateBatch(context.Background(), "tenant-1", "pk-1", "order.created", []string{"evt-1", "evt-1"})
	if err == nil {
		t.Fatal("CreateBatch accepted duplicate event IDs")
	}
	if len(events.envelopes) != 0 {
		t.Fatalf("processed %d batch envelopes for duplicate IDs", len(events.envelopes))
	}
}

func TestCreateBatchRejectsBatchAboveConfiguredMaximum(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	service := NewService(batches, events, domain.SystemClock{}, domain.BatchConfig{MaxBatch: 1})

	_, err := service.CreateBatch(context.Background(), "tenant-1", "pk-1", "order.created", []string{"evt-1", "evt-2"})
	if err == nil {
		t.Fatal("CreateBatch accepted a request larger than MaxBatch")
	}
	if len(events.envelopes) != 0 {
		t.Fatalf("processed %d batch envelopes for an oversized request", len(events.envelopes))
	}
}
