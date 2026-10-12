package batches

import (
	"errors"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type Service struct {
	batches       ports.BatchRepository
	events        ports.EventProcessor
	clock         ports.Clock
	cfg           domain.BatchConfig
	leaseProvider ports.ShardLeaseView
	workerID      string
	numShards     uint32
}

func NewService(
	batches ports.BatchRepository,
	events ports.EventProcessor,
	clock ports.Clock,
	cfg domain.BatchConfig,
) *Service {
	return &Service{
		batches:   batches,
		events:    events,
		clock:     clock,
		cfg:       cfg,
		numShards: 4096,
	}
}

// NewServiceWithLeaseProvider wires worker-owned shard fencing into batch
// execution. Batch-generated events must obey the same lease invariant as
// ordinary events; a synthetic token or fencing bypass is never permitted.
func NewServiceWithLeaseProvider(
	batches ports.BatchRepository,
	events ports.EventProcessor,
	clock ports.Clock,
	cfg domain.BatchConfig,
	leaseProvider ports.ShardLeaseView,
	workerID string,
	numShards uint32,
) *Service {
	service := NewService(batches, events, clock, cfg)
	service.leaseProvider = leaseProvider
	service.workerID = workerID
	if numShards > 0 {
		service.numShards = numShards
	}
	return service
}

func (s *Service) ownsPartition(tenantID, partitionKey string) bool {
	if s.leaseProvider == nil || tenantID == "" || partitionKey == "" {
		return false
	}
	envelope := &domain.EventEnvelope{TenantID: tenantID, PartitionKey: partitionKey}
	return s.leaseProvider.IsOwner(envelope.VirtualShard(s.numShards))
}

func (s *Service) Tick(ctx context.Context) (int, error) {
	if s.cfg.Mode == domain.BatchModeNone {
		return 0, nil
	}

	limit := s.cfg.MaxBatch * 10
	if limit <= 0 {
		limit = 400
	}

	entries, err := s.batches.ListUnbatched(ctx, limit)
	if err != nil {
		return 0, fmt.Errorf("list unbatched: %w", err)
	}

	if len(entries) == 0 {
		return 0, nil
	}

	type groupKey struct {
		tenantID     string
		partitionKey string
		ruleSet      string
	}
	groups := make(map[groupKey][]*domain.InboxEntry)
	order := make([]groupKey, 0)
	for _, e := range entries {
		if e.TenantID == "" || e.PartitionKey == "" || e.RuleSet == "" {
			continue
		}
		key := groupKey{tenantID: e.TenantID, partitionKey: e.PartitionKey, ruleSet: e.RuleSet}
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], e)
	}

	formed := 0
	for _, key := range order {
		members := groups[key]
		sort.SliceStable(members, func(i, j int) bool {
			return members[i].FirstSeenAt.Before(members[j].FirstSeenAt)
		})

		if !s.ready(members) {
			continue
		}
		// A worker only processes batches for shards whose lease it currently
		// owns. Other workers will handle those partitions on their own ticks.
		if !s.ownsPartition(members[0].TenantID, members[0].PartitionKey) {
			continue
		}

		n, err := s.processGroup(ctx, members)
		if err != nil {
			return formed, err
		}
		formed += n
	}

	return formed, nil
}

// CreateBatch creates a new batch from unbatched entries (manual batch formation)
func (s *Service) CreateBatch(ctx context.Context, tenantID, partitionKey, ruleSet string, eventIDs []string) (*domain.BatchRun, error) {
	if s.events == nil {
		return nil, domain.ErrBatchProcessingUnavailable
	}
	if len(eventIDs) == 0 {
		return nil, fmt.Errorf("no event IDs provided")
	}

	if tenantID == "" || partitionKey == "" || ruleSet == "" {
		return nil, fmt.Errorf("tenant, partition key, and rule set are required")
	}
	if s.cfg.MaxBatch > 0 && len(eventIDs) > s.cfg.MaxBatch {
		return nil, fmt.Errorf("requested batch size %d exceeds maximum %d", len(eventIDs), s.cfg.MaxBatch)
	}

	// Reject duplicate IDs instead of silently changing batch membership or identity.
	seen := make(map[string]struct{}, len(eventIDs))
	for _, id := range eventIDs {
		if id == "" {
			return nil, fmt.Errorf("event IDs must not be empty")
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("duplicate event ID %q", id)
		}
		seen[id] = struct{}{}
	}

	// Query by the exact requested IDs and business scope. A limited global
	// listing can otherwise return unrelated rows and silently omit requested IDs.
	members, err := s.batches.ListUnbatchedByIDs(ctx, tenantID, partitionKey, ruleSet, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("list requested unbatched events: %w", err)
	}
	if len(members) != len(seen) {
		found := make(map[string]struct{}, len(members))
		for _, member := range members {
			found[member.EventID] = struct{}{}
		}
		for id := range seen {
			if _, ok := found[id]; !ok {
				return nil, fmt.Errorf("requested event %q is missing, already batched, or outside the requested scope", id)
			}
		}
		return nil, fmt.Errorf("requested event set is incomplete")
	}

	sort.SliceStable(members, func(i, j int) bool {
		return members[i].FirstSeenAt.Before(members[j].FirstSeenAt)
	})

	_, err = s.processGroup(ctx, members)
	if err != nil {
		return nil, err
	}

	// Return the created batch run
	batchID := domain.ComputeBatchID(tenantID, partitionKey, ruleSet, eventIDs)
	return s.batches.GetRun(ctx, batchID)
}

// GetBatch retrieves a batch run by ID
func (s *Service) GetBatch(ctx context.Context, tenantID, batchID string) (*domain.BatchRun, error) {
	return s.batches.GetRunForTenant(ctx, tenantID, batchID)
}

// ListBatches lists batch runs with optional filters
func (s *Service) ListBatches(ctx context.Context, tenantID, ruleSet string, limit, offset int) ([]*domain.BatchRun, error) {
	return s.batches.ListRuns(ctx, tenantID, ruleSet, limit, offset)
}

func (s *Service) ready(members []*domain.InboxEntry) bool {
	switch s.cfg.Mode {
	case domain.BatchModeAccumulate:
		return s.cfg.MaxBatch > 0 && len(members) >= s.cfg.MaxBatch
	case domain.BatchModeWindow:
		oldest := members[0].FirstSeenAt
		return !s.clock.Now().Before(oldest.Add(s.cfg.Window))
	}
	return false
}

func (s *Service) processGroup(ctx context.Context, members []*domain.InboxEntry) (int, error) {
	if s.events == nil {
		return 0, domain.ErrBatchProcessingUnavailable
	}
	if len(members) == 0 {
		return 0, nil
	}

	batchSize := len(members)
	if s.cfg.MaxBatch > 0 && batchSize > s.cfg.MaxBatch {
		batchSize = s.cfg.MaxBatch
	}
	members = members[:batchSize]

	e0 := members[0]
	tenantID := e0.TenantID
	partitionKey := e0.PartitionKey
	ruleSet := e0.RuleSet

	eventIDs := make([]string, 0, len(members))
	batchEvents := make([]domain.BatchMember, 0, len(members))
	for _, m := range members {
		eventIDs = append(eventIDs, m.EventID)
		batchEvents = append(batchEvents, domain.BatchMember{
			ID:   m.EventID,
			Type: m.RuleSet,
			Data: m.Payload,
		})
	}

	batchID := domain.ComputeBatchID(tenantID, partitionKey, ruleSet, eventIDs)

	existing, err := s.batches.GetRun(ctx, batchID)
	if err != nil {
		return 0, fmt.Errorf("check existing batch: %w", err)
	}
	if existing != nil && existing.Status == domain.BatchRunStatusCompleted {
		// A previous attempt may have committed event processing but failed while
		// finalizing membership. CompleteBatch is idempotent for the same batch ID.
		if err := s.batches.CompleteBatch(ctx, existing, eventIDs); err != nil {
			return 0, fmt.Errorf("recover completed batch: %w", err)
		}
		return 0, nil
	}

	now := s.clock.Now()
	windowFrom := members[0].FirstSeenAt
	windowTo := now

	payload := domain.BatchData{
		BatchID:      batchID,
		TenantID:     tenantID,
		PartitionKey: partitionKey,
		RuleSet:      ruleSet,
		WindowFrom:   windowFrom,
		WindowTo:     windowTo,
		Events:       batchEvents,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("marshal batch data: %w", err)
	}

	envelope := &domain.EventEnvelope{
		ID:           batchID,
		Type:         domain.BatchRuleSet(ruleSet),
		TenantID:     tenantID,
		PartitionKey: partitionKey,
		OccurredAt:   now,
		Data:         data,
	}

	if s.leaseProvider == nil || s.workerID == "" {
		return 0, fmt.Errorf("batch processing requires a configured shard lease provider and worker ID")
	}
	shard := envelope.VirtualShard(s.numShards)
	if !s.leaseProvider.IsOwner(shard) {
		return 0, domain.ErrFencingTokenMismatch
	}
	fencingToken, ok := s.leaseProvider.GetFencingToken(shard)
	if !ok || fencingToken <= 0 {
		return 0, domain.ErrFencingTokenMismatch
	}
	exec, err := s.events.Process(ctx, envelope, fencingToken, shard, s.workerID)
	if err != nil {
		if errors.Is(err, domain.ErrRuleNotActive) || errors.Is(err, domain.ErrRuleNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("process batch: %w", err)
	}

	run := &domain.BatchRun{
		BatchID:      batchID,
		TenantID:     tenantID,
		PartitionKey: partitionKey,
		RuleSet:      ruleSet,
		Status:       domain.BatchRunStatusCompleted,
		MemberCount:  len(members),
		ExecutionID:  exec.ID,
		DecisionHash: exec.DecisionHash,
		WindowFrom:   &windowFrom,
		WindowTo:     &windowTo,
		CreatedAt:    now,
	}

	// Persist the run and all member associations atomically. If this fails
	// after event processing committed, the deterministic batch event can be
	// replayed safely and finalization retried.
	if err := s.batches.CompleteBatch(ctx, run, eventIDs); err != nil {
		return 0, fmt.Errorf("complete batch: %w", err)
	}

	return 1, nil
}
