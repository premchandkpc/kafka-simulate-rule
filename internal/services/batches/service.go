package batches

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type Service struct {
	batches   ports.BatchRepository
	events    ports.EventProcessor
	clock     ports.Clock
	cfg       domain.BatchConfig
}

func NewService(
	batches ports.BatchRepository,
	events ports.EventProcessor,
	clock ports.Clock,
	cfg domain.BatchConfig,
) *Service {
	return &Service{
		batches: batches,
		events:  events,
		clock:   clock,
		cfg:     cfg,
	}
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

	groups := make(map[string][]*domain.InboxEntry)
	order := make([]string, 0)
	for _, e := range entries {
		if e.TenantID == "" || e.PartitionKey == "" || e.RuleSet == "" {
			continue
		}
		key := e.TenantID + "|" + e.PartitionKey + "|" + e.RuleSet
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

		n, err := s.processGroup(ctx, members)
		if err != nil {
			return formed, err
		}
		formed += n
	}

	return formed, nil
}

func (s *Service) ready(members []*domain.InboxEntry) bool {
	switch s.cfg.Mode {
	case domain.BatchModeAccumulate:
		return len(members) >= s.cfg.MaxBatch
	case domain.BatchModeWindow:
		oldest := members[0].FirstSeenAt
		return !s.clock.Now().Before(oldest.Add(s.cfg.Window))
	}
	return false
}

func (s *Service) processGroup(ctx context.Context, members []*domain.InboxEntry) (int, error) {
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
		for _, m := range members {
			if err := s.batches.MarkBatched(ctx, m.TenantID, m.EventID, batchID); err != nil {
				return 0, fmt.Errorf("mark batched idempotent: %w", err)
			}
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

	exec, err := s.events.Process(ctx, envelope, 0, 0, "")
	if err != nil {
		if err == domain.ErrRuleNotActive || err == domain.ErrRuleNotFound {
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

	if err := s.batches.SaveRun(ctx, run); err != nil {
		return 0, fmt.Errorf("save batch run: %w", err)
	}

	for _, m := range members {
		if err := s.batches.MarkBatched(ctx, m.TenantID, m.EventID, batchID); err != nil {
			return 0, fmt.Errorf("mark batched: %w", err)
		}
	}

	return 1, nil
}