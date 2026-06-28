package executor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/premchand/flowrule/internal/dsl"
	"github.com/rs/zerolog"
)

// mock implementations

type mockCaller struct {
	mu      sync.Mutex
	calls   []string
	results map[string][]byte
	errs    map[string]error
}

func (m *mockCaller) Call(ctx context.Context, target string, body []byte) ([]byte, error) {
	m.mu.Lock()
	m.calls = append(m.calls, target)
	err := m.errs[target]
	resp := m.results[target]
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return []byte(fmt.Sprintf(`{"result":"ok","target":%q}`, target)), nil
	}
	return resp, nil
}

type mockBreaker struct {
	mu      sync.Mutex
	allowed map[string]bool
}

func (m *mockBreaker) Allow(target string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.allowed == nil {
		return true
	}
	return m.allowed[target]
}

func (m *mockBreaker) RecordSuccess(target string) {}
func (m *mockBreaker) RecordFailure(target string) {}

type mockCredits struct {
	mu      sync.Mutex
	blocked map[string]bool
}

func (m *mockCredits) CanSend(target string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.blocked == nil {
		return true
	}
	return !m.blocked[target]
}

func (m *mockCredits) Debit(target string)  {}
func (m *mockCredits) Credit(target string) {}

type mockEmitter struct {
	mu      sync.Mutex
	calls   []string
}

func (m *mockEmitter) Emit(ctx context.Context, target string, body []byte) error {
	m.mu.Lock()
	m.calls = append(m.calls, target)
	m.mu.Unlock()
	return nil
}

type testLogger struct {
	l zerolog.Logger
}

func newTestLogger() testLogger {
	return testLogger{l: zerolog.Nop()}
}

func (t testLogger) Info() *zerolog.Event  { return t.l.Info() }
func (t testLogger) Warn() *zerolog.Event  { return t.l.Warn() }
func (t testLogger) Error() *zerolog.Event { return t.l.Error() }
func (t testLogger) Debug() *zerolog.Event { return t.l.Debug() }

func newTestExecutor(caller *mockCaller, breaker *mockBreaker, credits *mockCredits, emitter *mockEmitter) *Executor {
	return New(caller, breaker, credits, emitter, newTestLogger())
}

func TestExecutorNextSuccess(t *testing.T) {
	caller := &mockCaller{}
	breaker := &mockBreaker{}
	credits := &mockCredits{}
	emitter := &mockEmitter{}
	exec := newTestExecutor(caller, breaker, credits, emitter)

	msg := &Message{Body: []byte(`{"id":1}`), LastResponse: []byte(`{"id":1}`)}
	plan := &dsl.ExecutionPlan{
		RuleID: "test",
		Instructions: []dsl.Instruction{
			{Op: dsl.OpNext, Targets: []string{"svc-a"}},
		},
	}

	err := exec.Execute(context.Background(), msg, plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(caller.calls) != 1 || caller.calls[0] != "svc-a" {
		t.Errorf("expected call to svc-a, got %v", caller.calls)
	}
	if msg.HopCount != 1 {
		t.Errorf("expected HopCount=1, got %d", msg.HopCount)
	}
	if msg.Failed() {
		t.Errorf("expected not failed")
	}
}

func TestExecutorNextFailureWithFallback(t *testing.T) {
	caller := &mockCaller{
		errs: map[string]error{
			"svc-a": errors.New("service error"),
			"svc-b": nil,
		},
		results: map[string][]byte{
			"svc-b": []byte(`{"fallback":"ok"}`),
		},
	}
	breaker := &mockBreaker{}
	credits := &mockCredits{}
	emitter := &mockEmitter{}
	exec := newTestExecutor(caller, breaker, credits, emitter)

	msg := &Message{Body: []byte(`{"id":1}`), LastResponse: []byte(`{"id":1}`)}
	plan := &dsl.ExecutionPlan{
		RuleID: "test",
		Instructions: []dsl.Instruction{
			{Op: dsl.OpNext, Targets: []string{"svc-a"}},
			{Op: dsl.OpFallback, Targets: []string{"svc-b"}},
		},
	}

	err := exec.Execute(context.Background(), msg, plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(caller.calls) != 2 {
		t.Fatalf("expected 2 calls (svc-a + fallback svc-b), got %v", caller.calls)
	}
	if caller.calls[0] != "svc-a" || caller.calls[1] != "svc-b" {
		t.Errorf("unexpected call order: %v", caller.calls)
	}
	if msg.Failed() {
		t.Errorf("expected fallback cleared failed state")
	}
}

func TestExecutorNextFailureWithoutFallback(t *testing.T) {
	caller := &mockCaller{
		errs: map[string]error{
			"svc-a": errors.New("service error"),
		},
	}
	breaker := &mockBreaker{}
	credits := &mockCredits{}
	emitter := &mockEmitter{}
	exec := newTestExecutor(caller, breaker, credits, emitter)

	msg := &Message{Body: []byte(`{"id":1}`), LastResponse: []byte(`{"id":1}`)}
	plan := &dsl.ExecutionPlan{
		RuleID: "test",
		Instructions: []dsl.Instruction{
			{Op: dsl.OpNext, Targets: []string{"svc-a"}},
		},
	}

	err := exec.Execute(context.Background(), msg, plan)
	if err == nil {
		t.Fatal("expected error when n: fails with no fallback")
	}
}

func TestExecutorRetryThenSuccess(t *testing.T) {
	callCount := 0
	var mu sync.Mutex

	breaker := &mockBreaker{}
	credits := &mockCredits{}
	emitter := &mockEmitter{}

	// Use a custom caller via the Executor directly
	exec := &Executor{
		caller: HTTPCallerFunc(func(ctx context.Context, target string, body []byte) ([]byte, error) {
			mu.Lock()
			callCount++
			count := callCount
			mu.Unlock()
			if count <= 2 {
				return nil, errors.New("transient error")
			}
			return []byte(`{"ok":true}`), nil
		}),
		breakers: breaker,
		credits:  credits,
		emitter:  emitter,
		log:      newTestLogger(),
	}

	msg := &Message{Body: []byte(`{"id":1}`), LastResponse: []byte(`{"id":1}`)}
	plan := &dsl.ExecutionPlan{
		RuleID: "test",
		Instructions: []dsl.Instruction{
			{Op: dsl.OpNext, Targets: []string{"svc-a"}, RetryN: 3},
		},
	}

	err := exec.Execute(context.Background(), msg, plan)
	if err != nil {
		t.Fatalf("expected retry to succeed, got error: %v", err)
	}
	if msg.Failed() {
		t.Errorf("expected success after retries")
	}
	if callCount != 3 {
		t.Errorf("expected 3 calls (1 initial + 2 retries), got %d", callCount)
	}
}

func TestExecutorDrop(t *testing.T) {
	caller := &mockCaller{}
	breaker := &mockBreaker{}
	credits := &mockCredits{}
	emitter := &mockEmitter{}
	exec := newTestExecutor(caller, breaker, credits, emitter)

	msg := &Message{Body: []byte(`{"status":"blocked"}`)}
	plan := &dsl.ExecutionPlan{
		RuleID: "test",
		Instructions: []dsl.Instruction{
			{Op: dsl.OpDrop},
		},
	}

	err := exec.Execute(context.Background(), msg, plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(caller.calls) != 0 {
		t.Errorf("expected no calls for d, got %v", caller.calls)
	}
}

func TestExecutorGatePass(t *testing.T) {
	caller := &mockCaller{}
	breaker := &mockBreaker{}
	credits := &mockCredits{}
	emitter := &mockEmitter{}
	exec := newTestExecutor(caller, breaker, credits, emitter)

	msg := &Message{Body: []byte(`{"amount":15000}`), LastResponse: []byte(`{"amount":15000}`)}
	plan := &dsl.ExecutionPlan{
		RuleID: "test",
		Instructions: []dsl.Instruction{
			{Op: dsl.OpGate, Operand: "amount", Operator: ">", Value: "10000"},
			{Op: dsl.OpNext, Targets: []string{"high-value-svc"}},
			{Op: dsl.OpPipe},
			{Op: dsl.OpNext, Targets: []string{"normal-svc"}},
		},
	}

	err := exec.Execute(context.Background(), msg, plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(caller.calls) != 1 || caller.calls[0] != "high-value-svc" {
		t.Errorf("expected call to high-value-svc, got %v", caller.calls)
	}
}

func TestExecutorGateSkip(t *testing.T) {
	caller := &mockCaller{}
	breaker := &mockBreaker{}
	credits := &mockCredits{}
	emitter := &mockEmitter{}
	exec := newTestExecutor(caller, breaker, credits, emitter)

	msg := &Message{Body: []byte(`{"amount":5000}`), LastResponse: []byte(`{"amount":5000}`)}
	plan := &dsl.ExecutionPlan{
		RuleID: "test",
		Instructions: []dsl.Instruction{
			{Op: dsl.OpGate, Operand: "amount", Operator: ">", Value: "10000"},
			{Op: dsl.OpNext, Targets: []string{"high-value-svc"}},
			{Op: dsl.OpPipe},
			{Op: dsl.OpNext, Targets: []string{"normal-svc"}},
		},
	}

	err := exec.Execute(context.Background(), msg, plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(caller.calls) != 1 || caller.calls[0] != "normal-svc" {
		t.Errorf("expected call to normal-svc, got %v", caller.calls)
	}
}

func TestExecutorGateWithFallback(t *testing.T) {
	caller := &mockCaller{
		errs: map[string]error{
			"auto-approve": errors.New("service error"),
			"review-queue": nil,
		},
		results: map[string][]byte{
			"review-queue": []byte(`{"status":"queued"}`),
		},
	}
	breaker := &mockBreaker{}
	credits := &mockCredits{}
	emitter := &mockEmitter{}
	exec := newTestExecutor(caller, breaker, credits, emitter)

	msg := &Message{Body: []byte(`{"amount":5000}`), LastResponse: []byte(`{"amount":5000}`)}
	plan := &dsl.ExecutionPlan{
		RuleID: "test",
		Instructions: []dsl.Instruction{
			{Op: dsl.OpGate, Operand: "amount", Operator: ">", Value: "10000"},
			{Op: dsl.OpNext, Targets: []string{"manual-review"}},
			{Op: dsl.OpPipe},
			{Op: dsl.OpNext, Targets: []string{"auto-approve"}, TimeoutMs: 500},
			{Op: dsl.OpFallback, Targets: []string{"review-queue"}},
		},
	}

	err := exec.Execute(context.Background(), msg, plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(caller.calls) != 2 || caller.calls[0] != "auto-approve" || caller.calls[1] != "review-queue" {
		t.Errorf("expected calls to auto-approve then review-queue, got %v", caller.calls)
	}
}

func TestExecutorParallelCollect(t *testing.T) {
	caller := &mockCaller{
		results: map[string][]byte{
			"fraud":     []byte(`{"fraud":false}`),
			"inventory": []byte(`{"stock":10}`),
		},
	}
	breaker := &mockBreaker{}
	credits := &mockCredits{}
	emitter := &mockEmitter{}
	exec := newTestExecutor(caller, breaker, credits, emitter)

	msg := &Message{Body: []byte(`{"id":1}`), LastResponse: []byte(`{"id":1}`)}
	plan := &dsl.ExecutionPlan{
		RuleID: "test",
		Instructions: []dsl.Instruction{
			{Op: dsl.OpParallel, Targets: []string{"fraud", "inventory"}},
			{Op: dsl.OpCollect},
		},
	}

	err := exec.Execute(context.Background(), msg, plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.Failed() {
		t.Errorf("expected parallel collect success")
	}
	// LastResponse should be a JSON array
	if len(msg.LastResponse) == 0 || msg.LastResponse[0] != '[' {
		t.Errorf("expected JSON array, got %s", string(msg.LastResponse))
	}
}

func TestExecutorMap(t *testing.T) {
	caller := &mockCaller{}
	breaker := &mockBreaker{}
	credits := &mockCredits{}
	emitter := &mockEmitter{}
	exec := newTestExecutor(caller, breaker, credits, emitter)

	msg := &Message{Body: []byte(`{"order_id":"ORD-123","amount":5000}`), LastResponse: []byte(`{"order_id":"ORD-123","amount":5000}`)}
	plan := &dsl.ExecutionPlan{
		RuleID: "test",
		Instructions: []dsl.Instruction{
			{
				Op: dsl.OpMap,
				MapExpr: &dsl.MapExpr{
					FieldPath: []string{"order_id"},
				},
			},
		},
	}

	err := exec.Execute(context.Background(), msg, plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(msg.LastResponse) != `"ORD-123"` {
		t.Errorf("expected mapped result %q, got %q", `"ORD-123"`, string(msg.LastResponse))
	}
}

func TestExecutorFullPipeline(t *testing.T) {
	caller := &mockCaller{
		results: map[string][]byte{
			"validate":  []byte(`{"valid":true}`),
			"fraud":     []byte(`{"fraud":false}`),
			"inventory": []byte(`{"stock":10}`),
			"fulfill":   []byte(`{"status":"fulfilled"}`),
		},
	}
	breaker := &mockBreaker{}
	credits := &mockCredits{}
	emitter := &mockEmitter{}
	exec := newTestExecutor(caller, breaker, credits, emitter)

	msg := &Message{Body: []byte(`{"id":1}`), LastResponse: []byte(`{"id":1}`)}
	plan := &dsl.ExecutionPlan{
		RuleID: "order-pipeline",
		Instructions: []dsl.Instruction{
			{Op: dsl.OpNext, Targets: []string{"validate"}, TimeoutMs: 500},
			{Op: dsl.OpParallel, Targets: []string{"fraud", "inventory"}, TimeoutMs: 1000},
			{Op: dsl.OpCollect},
			{Op: dsl.OpFallback, Targets: []string{"dlq"}},
			{Op: dsl.OpNext, Targets: []string{"fulfill"}},
			{Op: dsl.OpEmit, Targets: []string{"notify", "analytics"}},
		},
	}

	err := exec.Execute(context.Background(), msg, plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Validate called first
	if len(caller.calls) < 4 {
		t.Fatalf("expected at least 4 calls, got %d: %v", len(caller.calls), caller.calls)
	}
	if caller.calls[0] != "validate" {
		t.Errorf("expected first call to validate, got %s", caller.calls[0])
	}
	if msg.HopCount != 3 {
		t.Errorf("expected HopCount=3 (validate + parallel + fulfill), got %d", msg.HopCount)
	}
	if msg.Failed() {
		t.Errorf("expected pipeline success")
	}

	// Wait a tiny bit for emit goroutines
	time.Sleep(10 * time.Millisecond)
	if len(emitter.calls) != 2 {
		t.Errorf("expected 2 emit calls, got %d: %v", len(emitter.calls), emitter.calls)
	}
}
