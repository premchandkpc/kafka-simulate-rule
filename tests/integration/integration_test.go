package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/flowrule/flowrule/internal/rules"
	svcevents "github.com/flowrule/flowrule/internal/services/events"
	svcrules "github.com/flowrule/flowrule/internal/services/rules"
)

type mockInbox struct {
	entries map[string]*domain.InboxEntry
}

func newMockInbox() *mockInbox {
	return &mockInbox{entries: make(map[string]*domain.InboxEntry)}
}

func (m *mockInbox) Insert(ctx context.Context, entry *domain.InboxEntry) (bool, error) {
	key := entry.TenantID + ":" + entry.EventID
	if _, exists := m.entries[key]; exists {
		return false, nil
	}
	m.entries[key] = entry
	return true, nil
}

func (m *mockInbox) Get(ctx context.Context, tenantID string, eventID string) (*domain.InboxEntry, error) {
	key := tenantID + ":" + eventID
	return m.entries[key], nil
}

func (m *mockInbox) MarkCommitted(ctx context.Context, tenantID string, eventID string, executionID string) error {
	key := tenantID + ":" + eventID
	if entry, ok := m.entries[key]; ok {
		entry.Status = domain.InboxStatusCommitted
		entry.ExecutionID = executionID
		now := time.Now().UTC()
		entry.CommittedAt = &now
	}
	return nil
}

type mockActivation struct {
	activations map[string]*domain.RuleActivation
}

func newMockActivation() *mockActivation {
	return &mockActivation{activations: make(map[string]*domain.RuleActivation)}
}

func (m *mockActivation) Get(ctx context.Context, tenantScope string, ruleSet string) (*domain.RuleActivation, error) {
	key := tenantScope + ":" + ruleSet
	return m.activations[key], nil
}

func (m *mockActivation) Set(ctx context.Context, activation *domain.RuleActivation) error {
	key := activation.TenantScope + ":" + activation.RuleSet
	m.activations[key] = activation
	return nil
}

func (m *mockActivation) Delete(ctx context.Context, tenantScope string, ruleSet string) error {
	key := tenantScope + ":" + ruleSet
	delete(m.activations, key)
	return nil
}

type mockRuleRepo struct {
	revisions map[string]*domain.RuleRevision
}

func newMockRuleRepo() *mockRuleRepo {
	return &mockRuleRepo{revisions: make(map[string]*domain.RuleRevision)}
}

func (m *mockRuleRepo) GetActive(ctx context.Context, tenantScope string, ruleSet string) (*domain.RuleRevision, error) {
	key := tenantScope + ":" + ruleSet
	return m.revisions[key], nil
}

func (m *mockRuleRepo) Save(ctx context.Context, revision *domain.RuleRevision) error {
	key := revision.TenantScope + ":" + revision.RuleID
	m.revisions[key] = revision
	return nil
}

func (m *mockRuleRepo) Delete(ctx context.Context, tenantScope, ruleSet string, revision int64) error {
	key := tenantScope + ":" + ruleSet
	delete(m.revisions, key)
	return nil
}

func (m *mockRuleRepo) Get(ctx context.Context, tenantScope, ruleSet string, revision int64) (*domain.RuleRevision, error) {
	key := tenantScope + ":" + ruleSet
	return m.revisions[key], nil
}

func (m *mockRuleRepo) List(ctx context.Context, tenantScope string) ([]*domain.RuleRevision, error) {
	var result []*domain.RuleRevision
	for _, rev := range m.revisions {
		result = append(result, rev)
	}
	return result, nil
}

type mockExecutionRepo struct {
	executions map[string]*domain.Execution
}

func newMockExecutionRepo() *mockExecutionRepo {
	return &mockExecutionRepo{executions: make(map[string]*domain.Execution)}
}

func (m *mockExecutionRepo) Save(ctx context.Context, execution *domain.Execution) error {
	m.executions[execution.ID] = execution
	return nil
}

func (m *mockExecutionRepo) Get(ctx context.Context, executionID string) (*domain.Execution, error) {
	return m.executions[executionID], nil
}

func (m *mockExecutionRepo) UpdateStatus(ctx context.Context, executionID string, status domain.ExecutionStatus, errMsg string) error {
	if exec, ok := m.executions[executionID]; ok {
		exec.Status = status
		exec.Error = errMsg
		if status == domain.ExecutionStatusCompleted || status == domain.ExecutionStatusFailed {
			now := time.Now().UTC()
			exec.CompletedAt = &now
		}
	}
	return nil
}

type mockOutbox struct {
	effects map[string]*domain.OutboxEffect
}

func newMockOutbox() *mockOutbox {
	return &mockOutbox{effects: make(map[string]*domain.OutboxEffect)}
}

func (m *mockOutbox) Insert(ctx context.Context, effects []domain.OutboxEffect) error {
	for _, ef := range effects {
		m.effects[ef.ID] = &ef
	}
	return nil
}

func (m *mockOutbox) ClaimPending(ctx context.Context, batchSize int, owner string, claimTTL time.Duration) ([]domain.OutboxEffect, error) {
	var claimed []domain.OutboxEffect
	for _, ef := range m.effects {
		if ef.Status == domain.OutboxStatusPending && !ef.AvailableAt.After(time.Now().UTC()) {
			claimed = append(claimed, *ef)
			if len(claimed) >= batchSize {
				break
			}
		}
	}
	return claimed, nil
}

func (m *mockOutbox) MarkDelivered(ctx context.Context, effectIDs []string, claimant string) error {
	for _, effectID := range effectIDs {
		if ef, ok := m.effects[effectID]; ok {
			ef.Status = domain.OutboxStatusDelivered
		}
	}
	return nil
}

func (m *mockOutbox) ScheduleRetry(ctx context.Context, effectID string, delay time.Duration, attempts int, errMsg string) error {
	if ef, ok := m.effects[effectID]; ok {
		ef.Attempts = attempts
		ef.LastError = errMsg
		ef.AvailableAt = time.Now().UTC().Add(delay)
	}
	return nil
}

func (m *mockOutbox) Quarantine(ctx context.Context, effectID string, errMsg string) error {
	if ef, ok := m.effects[effectID]; ok {
		ef.Status = domain.OutboxStatusQuarantined
		ef.LastError = errMsg
	}
	return nil
}

func (m *mockOutbox) GetPending(ctx context.Context, limit int) ([]domain.OutboxEffect, error) {
	var pending []domain.OutboxEffect
	for _, ef := range m.effects {
		if ef.Status == domain.OutboxStatusPending {
			pending = append(pending, *ef)
			if len(pending) >= limit {
				break
			}
		}
	}
	return pending, nil
}

func (m *mockOutbox) GetByExecution(ctx context.Context, executionID string) ([]domain.OutboxEffect, error) {
	var result []domain.OutboxEffect
	for _, ef := range m.effects {
		if ef.ExecutionID == executionID {
			result = append(result, *ef)
		}
	}
	return result, nil
}

type mockQuarantine struct {
	entries map[string]*domain.QuarantineEntry
}

func newMockQuarantine() *mockQuarantine {
	return &mockQuarantine{entries: make(map[string]*domain.QuarantineEntry)}
}

func (m *mockQuarantine) Save(ctx context.Context, entry *domain.QuarantineEntry) error {
	m.entries[entry.ID] = entry
	return nil
}

func (m *mockQuarantine) Get(ctx context.Context, id string) (*domain.QuarantineEntry, error) {
	return m.entries[id], nil
}

func (m *mockQuarantine) List(ctx context.Context, filter ports.QuarantineFilter) ([]*domain.QuarantineEntry, error) {
	var result []*domain.QuarantineEntry
	for _, entry := range m.entries {
		result = append(result, entry)
		if filter.Limit > 0 && len(result) >= filter.Limit {
			break
		}
	}
	return result, nil
}

func (m *mockQuarantine) Replay(ctx context.Context, id string) error {
	return nil
}

func (m *mockQuarantine) Delete(ctx context.Context, id string) error {
	delete(m.entries, id)
	return nil
}

func setupTestUseCase(t *testing.T) (*svcevents.Service, *mockInbox, *mockExecutionRepo, *mockOutbox) {
	t.Helper()
	compiler := rules.NewCompiler(rules.DefaultLimits())
	evaluator := rules.NewEvaluator()
	clock := domain.SystemClock{}

	inbox := newMockInbox()
	activations := newMockActivation()
	ruleRepo := newMockRuleRepo()
	executions := newMockExecutionRepo()
	outbox := newMockOutbox()
	quarantine := newMockQuarantine()

	mockTx := &mockTx{
		inbox:       inbox,
		activations: activations,
		ruleRepo:    ruleRepo,
		executions:  executions,
		outbox:      outbox,
	}
	txManager := newMockTxManager(mockTx)

	newRepos := func(tx ports.Transaction) (svcevents.TxRepos, error) {
		return svcevents.TxRepos{
			Inbox:       inbox,
			Activations: activations,
			RuleRepo:    ruleRepo,
			Executions:  executions,
			Outbox:      outbox,
			ShardLeases: newMockShardLease(),
		}, nil
	}

	uc := svcevents.NewService(
		compiler, evaluator, quarantine, clock, txManager, newRepos,
	)

	revision, err := compiler.Compile(json.RawMessage(`{
		"rule_set": "order.created",
		"revision": 1,
		"mode": "first_match",
		"rules": [{
			"id": "high-value",
			"priority": 100,
			"when": {
				"path": "$.total",
				"op": "gte",
				"value": 1000
			},
			"then": [{
				"emit": {
					"topic": "orders.review",
					"data": {"reason": "high_value"}
				}
			}]
		}]
	}`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	if err := activations.Set(context.Background(), &domain.RuleActivation{
		TenantScope: "test-tenant",
		RuleSet:     "order.created",
		Revision:    1,
		Version:     1,
		Actor:       "test",
	}); err != nil {
		t.Fatalf("set activation: %v", err)
	}
	ruleRepo.revisions["test-tenant:order.created"] = revision

	return uc, inbox, executions, outbox
}

type mockTx struct {
	inbox       *mockInbox
	activations *mockActivation
	ruleRepo    *mockRuleRepo
	executions  *mockExecutionRepo
	outbox      *mockOutbox
	ctx         context.Context
}

func (m *mockTx) Commit(ctx context.Context) error   { return nil }
func (m *mockTx) Rollback(ctx context.Context) error { return nil }
func (m *mockTx) Context() context.Context           { return m.ctx }
func (m *mockTx) FencingToken() int64                { return 0 }
func (m *mockTx) SetFencingToken(token int64)        {}

type mockTxManager struct {
	tx *mockTx
}

func newMockTxManager(tx *mockTx) *mockTxManager {
	return &mockTxManager{tx: tx}
}

func (m *mockTxManager) Begin(ctx context.Context) (ports.Transaction, error) {
	m.tx.ctx = ctx
	return m.tx, nil
}

func (m *mockTxManager) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

type mockShardLease struct{}

func newMockShardLease() *mockShardLease {
	return &mockShardLease{}
}

func (m *mockShardLease) Acquire(ctx context.Context, shard uint32, owner string, ttl time.Duration) (*domain.ShardLease, error) {
	return &domain.ShardLease{VirtualShard: shard, Owner: owner, FencingToken: 1}, nil
}

func (m *mockShardLease) Renew(ctx context.Context, shard uint32, owner string, fencingToken int64, ttl time.Duration) (*domain.ShardLease, error) {
	return &domain.ShardLease{VirtualShard: shard, Owner: owner, FencingToken: fencingToken}, nil
}

func (m *mockShardLease) Release(ctx context.Context, shard uint32, owner string) error {
	return nil
}

func (m *mockShardLease) GetOwner(ctx context.Context, shard uint32) (*domain.ShardLease, error) {
	return &domain.ShardLease{VirtualShard: shard, Owner: "test", FencingToken: 1}, nil
}

func (m *mockShardLease) ValidateFencingToken(ctx context.Context, shard uint32, owner string, fencingToken int64) error {
	return nil
}

func (m *mockShardLease) AssertHeld(ctx context.Context, shard uint32, owner string, fencingToken int64) error {
	return nil
}

func TestDuplicateRedeliveryProducesOneExecution(t *testing.T) {
	uc, _, executions, _ := setupTestUseCase(t)

	env := &domain.EventEnvelope{
		ID:           "evt-dup-1",
		Type:         "order.created",
		TenantID:     "test-tenant",
		PartitionKey: "order-1",
		OccurredAt:   time.Now().UTC(),
		Data:         json.RawMessage(`{"total": 1500, "id": "order-1"}`),
	}

	exec1, err := uc.Process(context.Background(), env, 1, 1, "test-worker")
	if err != nil {
		t.Fatalf("first process: %v", err)
	}
	if exec1 == nil {
		t.Fatal("expected execution from first process")
	}

	exec2, err := uc.Process(context.Background(), env, 1, 1, "test-worker")
	if err != nil {
		t.Fatalf("second process: %v", err)
	}
	if exec2 == nil {
		t.Fatal("expected execution from second process")
	}

	if exec1.ID != exec2.ID {
		t.Errorf("expected same execution ID, got %s and %s", exec1.ID, exec2.ID)
	}

	totalExecutions := 0
	for _, exec := range executions.executions {
		if exec.EventID == "evt-dup-1" {
			totalExecutions++
		}
	}
	if totalExecutions != 1 {
		t.Errorf("expected 1 execution for event, got %d", totalExecutions)
	}
}

func TestDeterministicHashConsistency(t *testing.T) {
	uc, _, _, _ := setupTestUseCase(t)

	env := &domain.EventEnvelope{
		ID:           "evt-det-1",
		Type:         "order.created",
		TenantID:     "test-tenant",
		PartitionKey: "order-2",
		OccurredAt:   time.Now().UTC(),
		Data:         json.RawMessage(`{"total": 2000, "id": "order-2"}`),
	}

	exec1, _ := uc.Process(context.Background(), env, 1, 1, "test-worker")

	// Process same event again - should get same execution and hash
	exec2, _ := uc.Process(context.Background(), env, 1, 1, "test-worker")

	if exec1.DecisionHash == "" {
		t.Error("expected non-empty decision hash")
	}
	if exec2.DecisionHash == "" {
		t.Error("expected non-empty decision hash")
	}
	if exec1.DecisionHash != exec2.DecisionHash {
		t.Errorf("expected same decision hash for same event: %s != %s", exec1.DecisionHash, exec2.DecisionHash)
	}
	if exec1.ID != exec2.ID {
		t.Errorf("expected same execution ID for same event: %s != %s", exec1.ID, exec2.ID)
	}
}

func TestEffectIDGenerationIsDeterministic(t *testing.T) {
	id1 := domain.ComputeEffectID("tenant", "event-1", "rule-set", 1, "rule-1", 0)
	id2 := domain.ComputeEffectID("tenant", "event-1", "rule-set", 1, "rule-1", 0)

	if id1 != id2 {
		t.Errorf("expected same effect ID: %s != %s", id1, id2)
	}

	id3 := domain.ComputeEffectID("tenant", "event-1", "rule-set", 1, "rule-1", 1)
	if id1 == id3 {
		t.Error("expected different effect ID for different action index")
	}
}

func TestActivateRuleUsesExplicitTenantScope(t *testing.T) {
	compiler := rules.NewCompiler(rules.DefaultLimits())
	ruleRepo := newMockRuleRepo()
	activations := newMockActivation()
	executions := newMockExecutionRepo()
	svc := svcrules.NewService(compiler, ruleRepo, activations, executions, domain.SystemClock{})

	_, err := svc.Activate(context.Background(), "tenant-a", "order.created", json.RawMessage(`{
		"rule_set":"order.created", "revision":1, "mode":"first_match",
		"rules":[{"id":"match", "priority":1,
		"when":{"path":"$.total", "op":"gte", "value":1},
		"then":[{"emit":{"topic":"orders.review", "data":{}}}]}]
	}`), "test")
	if err != nil {
		t.Fatalf("activate rule: %v", err)
	}
	if ruleRepo.revisions["tenant-a:order.created"] == nil {
		t.Fatal("expected rule revision to be saved under the supplied tenant scope")
	}
	if ruleRepo.revisions["order.created:order.created"] != nil {
		t.Fatal("rule revision must not derive tenant scope from the rule ID")
	}
}

func TestGivenActiveRule_WhenEventArrives_ThenExecutionAndOutboxAreCreated(t *testing.T) {
	uc, inbox, executions, outbox := setupTestUseCase(t)
	env := &domain.EventEnvelope{
		ID:           "evt-given-1",
		Type:         "order.created",
		TenantID:     "test-tenant",
		PartitionKey: "order-42",
		OccurredAt:   time.Now().UTC(),
		Data:         json.RawMessage(`{"total": 1500, "id": "order-42"}`),
	}

	exec, err := uc.Process(context.Background(), env, 1, 1, "test-worker")
	if err != nil {
		t.Fatalf("process event: %v", err)
	}
	if exec == nil {
		t.Fatal("expected execution")
	}
	if inbox.entries["test-tenant:evt-given-1"].Status != domain.InboxStatusCommitted {
		t.Fatal("expected inbox entry to be committed")
	}
	if len(executions.executions) != 1 {
		t.Fatalf("expected one execution, got %d", len(executions.executions))
	}
	if len(outbox.effects) != 1 {
		t.Fatalf("expected one outbox effect, got %d", len(outbox.effects))
	}
}
