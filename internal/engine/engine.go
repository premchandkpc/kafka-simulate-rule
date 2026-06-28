package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/premchand/flowrule/internal/executor"
	"github.com/premchand/flowrule/internal/memory"
	"github.com/premchand/flowrule/internal/reliability"
)

// IdempotencyStore deduplicates messages by a configurable key field.
type IdempotencyStore struct {
	mu      sync.Mutex
	entries map[string]time.Time
}

// NewIdempotencyStore creates a new idempotency store.
func NewIdempotencyStore() *IdempotencyStore {
	return &IdempotencyStore{
		entries: make(map[string]time.Time),
	}
}

// CheckAndSet returns true if key was seen within its TTL window.
func (s *IdempotencyStore) CheckAndSet(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if exp, ok := s.entries[key]; ok && time.Now().Before(exp) {
		return true
	}
	s.entries[key] = time.Now().Add(ttl)
	return false
}

// StartCleanup launches a background goroutine that removes expired entries.
func (s *IdempotencyStore) StartCleanup(ctx context.Context) {
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.mu.Lock()
				now := time.Now()
				for k, exp := range s.entries {
					if now.After(exp) {
						delete(s.entries, k)
					}
				}
				s.mu.Unlock()
			}
		}
	}()
}

// Engine is the top-level runtime for FlowRule.
type Engine struct {
	table    atomic.Pointer[RuleTable]
	exec     *executor.Executor
	workers  *PartitionPool
	dlq      DLQClient
	idempot  *IdempotencyStore
	credits  CreditsClient
	breakers BreakersClient
	intern   *memory.InternTable
	metrics  MetricsClient
	wg       sync.WaitGroup
	started  atomic.Bool
	stopped  atomic.Bool
	mu       sync.Mutex
}

// DLQClient persists failed messages.
type DLQClient interface {
	Push(snap *reliability.DLQSnapshot) error
}

// IdempotencyClient checks for duplicate messages.
type IdempotencyClient interface {
	CheckAndSet(key string, ttl time.Duration) bool
	StartCleanup(ctx context.Context)
}

// CreditsClient manages per-target backpressure.
type CreditsClient interface {
	Register(target string)
	CanSend(target string) bool
}

// BreakersClient manages circuit breakers.
type BreakersClient interface {
	Register(target string)
}

// MetricsClient collects and exposes metrics.
type MetricsClient interface {
	IncMessages(ruleID, result string)
	ObserveLatency(ruleID string, dur time.Duration)
}

// EngineConfig configures the engine.
type EngineConfig struct {
	WorkerCount  int
	CreditsLimit int32
}

// PartitionPool routes messages by partition key.
type PartitionPool struct {
	workers []*partitionWorker
	n       uint32
}

type partitionWorker struct {
	id    int
	inbox chan *executor.Message
	exec  func(context.Context, *executor.Message) error
}

func newPartitionPool(n int, exec func(context.Context, *executor.Message) error) *PartitionPool {
	pool := &PartitionPool{
		workers: make([]*partitionWorker, n),
		n:       uint32(n),
	}
	for i := range pool.workers {
		pool.workers[i] = &partitionWorker{
			id:    i,
			inbox: make(chan *executor.Message, 1024),
			exec:  exec,
		}
	}
	return pool
}

func (p *PartitionPool) start(ctx context.Context) {
	for _, w := range p.workers {
		w := w
		go w.run(ctx)
	}
}

func (p *PartitionPool) submit(msg *executor.Message) {
	key := msg.PartitionKey
	if key == "" {
		key = fmt.Sprintf("%d", msg.ID)
	}
	idx := fnv32a(key) % p.n
	p.workers[idx].inbox <- msg
}

func (p *PartitionPool) drain(ctx context.Context) {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			all := true
			for _, w := range p.workers {
				if len(w.inbox) > 0 {
					all = false
					break
				}
			}
			if all {
				return
			}
		}
	}
}

func (w *partitionWorker) run(ctx context.Context) {
	for {
		select {
		case msg := <-w.inbox:
			_ = w.exec(ctx, msg)
			msg.Release()
		case <-ctx.Done():
			return
		}
	}
}

func fnv32a(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// New creates a new Engine.
func New(cfg EngineConfig) *Engine {
	return &Engine{}
}

// SetExecutor injects the executor and its dependencies after creation.
func (e *Engine) SetExecutor(exec *executor.Executor) {
	e.exec = exec
}

// SetInternTable injects the intern table.
func (e *Engine) SetInternTable(intern *memory.InternTable) {
	e.intern = intern
}

// SetDependencies injects remaining dependencies.
func (e *Engine) SetDependencies(dlq DLQClient, idempot *IdempotencyStore, credits CreditsClient, breakers BreakersClient, metrics MetricsClient) {
	e.dlq = dlq
	e.idempot = idempot
	e.credits = credits
	e.breakers = breakers
	e.metrics = metrics
}

// Start begins processing.
func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started.Load() {
		return fmt.Errorf("engine: already started")
	}

	if e.exec == nil {
		return fmt.Errorf("engine: executor not set")
	}

	workers := newPartitionPool(8, e.dispatch)
	e.workers = workers
	workers.start(ctx)

	if e.idempot != nil {
		e.idempot.StartCleanup(ctx)
	}

	e.started.Store(true)
	return nil
}

// Match returns the first rule matching msg.Body, or nil.
func (e *Engine) Match(body []byte) *CompiledRule {
	table := e.table.Load()
	if table == nil {
		return nil
	}
	for _, rule := range table.rules {
		if rule.Matcher(body) {
			return rule
		}
	}
	return nil
}

// Submit sends a message to the engine for processing.
func (e *Engine) Submit(ctx context.Context, msg *executor.Message) error {
	msg.Release()
	return nil
}

// dispatch is called by the worker pool for each message.
func (e *Engine) dispatch(ctx context.Context, msg *executor.Message) error {
	// Find matching rule
	rule := e.Match(msg.Body)
	if rule == nil {
		if e.metrics != nil {
			e.metrics.IncMessages("unknown", "dropped")
		}
		return nil
	}

	msg.RuleID = rule.ID
	msg.PlanVersion = rule.Plan.Version

	// Idempotency check
	if rule.Idempotency != nil && rule.Idempotency.Enabled {
		key := extractField(msg.Body, rule.Idempotency.KeyField)
		if key != "" {
			ttl := time.Duration(rule.Idempotency.TTLSecond) * time.Second
			if e.idempot != nil && e.idempot.CheckAndSet(key, ttl) {
				if e.metrics != nil {
					e.metrics.IncMessages(rule.ID, "duplicate")
				}
				return nil
			}
		}
	}

	// Execute pipeline
	start := time.Now()
	err := e.exec.Execute(ctx, msg, rule.Plan)
	dur := time.Since(start)

	if e.metrics != nil {
		if err != nil {
			e.metrics.IncMessages(rule.ID, "failure")
		} else {
			e.metrics.IncMessages(rule.ID, "success")
		}
		e.metrics.ObserveLatency(rule.ID, dur)
	}

	if err != nil {
		// Send to DLQ
		if e.dlq != nil {
			snap := NewDLQSnapshot(msg, err.Error(), msg.Stage)
			if pushErr := e.dlq.Push(snap); pushErr != nil {
				return fmt.Errorf("engine: pipeline error and DLQ push failed: %v (original: %w)", pushErr, err)
			}
		}
		return fmt.Errorf("engine: pipeline failed: %w", err)
	}

	return nil
}

// Reload atomically replaces the rule table.
func (e *Engine) Reload(table *RuleTable) {
	e.table.Store(table)
}

// Shutdown gracefully stops the engine.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.started.Load() {
		return nil
	}

	e.stopped.Store(true)
	if e.workers != nil {
		e.workers.drain(ctx)
	}

	e.wg.Wait()
	return nil
}

// extractField gets a string value from JSON body.
func extractField(body []byte, field string) string {
	if field == "" || len(body) == 0 {
		return ""
	}
	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		return ""
	}
	v, ok := data[field]
	if !ok {
		return ""
	}
	return fmt.Sprint(v)
}

// NewDLQSnapshot creates a DLQ snapshot from a failed message.
func NewDLQSnapshot(msg *executor.Message, errMsg, failedStage string) *reliability.DLQSnapshot {
	return &reliability.DLQSnapshot{
		ID:            msg.ID,
		CorrelationID: msg.CorrelationID,
		RuleID:        msg.RuleID,
		PlanVersion:   msg.PlanVersion,
		Body:          msg.Body,
		ContentType:   msg.ContentType,
		ReceivedAt:    msg.ReceivedAt,
		FailedAt:      time.Now(),
		FailedStage:   failedStage,
		ErrorChain:    msg.Errors,
	}
}
