package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/premchand/flowrule/internal/dsl"
	"github.com/premchand/flowrule/internal/engine"
	"github.com/premchand/flowrule/internal/executor"
	"github.com/premchand/flowrule/internal/flow"
	"github.com/premchand/flowrule/internal/reliability"
	"github.com/premchand/flowrule/internal/transport"
	"github.com/rs/zerolog"
)

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

func TestFullPipelineEndToEnd(t *testing.T) {
	validateSvc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"valid":true}`))
	}))
	defer validateSvc.Close()

	fraudSvc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"fraud":false}`))
	}))
	defer fraudSvc.Close()

	inventorySvc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"stock":10}`))
	}))
	defer inventorySvc.Close()

	fulfillSvc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"fulfilled"}`))
	}))
	defer fulfillSvc.Close()

	dlqSvc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"dlq":"stored"}`))
	}))
	defer dlqSvc.Close()

	notifySvc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"notified":true}`))
	}))
	defer notifySvc.Close()

	registry := dsl.TargetRegistry{
		"validate":  validateSvc.URL,
		"fraud":     fraudSvc.URL,
		"inventory": inventorySvc.URL,
		"fulfill":   fulfillSvc.URL,
		"dlq":       dlqSvc.URL,
		"notify":    notifySvc.URL,
	}

	pipeline := "t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify"
	plan, err := compilePipeline(pipeline, registry, "order-pipeline", 1)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	caller := transport.NewHTTPCaller(5 * time.Second)
	breakers := reliability.NewBreakerRegistry(5, 30*time.Second)
	credits := flow.NewCreditController(100)
	exec := executor.New(caller, breakers, credits, caller, newTestLogger())

	msg := &executor.Message{
		Body:         []byte(`{"type":"ORDER","id":1}`),
		LastResponse: []byte(`{"type":"ORDER","id":1}`),
		ReceivedAt:   time.Now(),
	}

	ctx := context.Background()
	err = exec.Execute(ctx, msg, plan)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	if msg.HopCount != 3 {
		t.Errorf("expected HopCount=3, got %d", msg.HopCount)
	}
	if msg.Failed() {
		t.Errorf("expected pipeline success")
	}
}

func TestGatePipelineEndToEnd(t *testing.T) {
	manualSvc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"review":"manual"}`))
	}))
	defer manualSvc.Close()

	autoSvc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"approved":true}`))
	}))
	defer autoSvc.Close()

	fallbackSvc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"queued":true}`))
	}))
	defer fallbackSvc.Close()

	registry := dsl.TargetRegistry{
		"manual-review": manualSvc.URL,
		"auto-approve":  autoSvc.URL,
		"review-queue":  fallbackSvc.URL,
	}

	plan, err := compilePipeline("g:amount>10000 n:manual-review | t500 n:auto-approve f:review-queue", registry, "high-value", 1)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	caller := transport.NewHTTPCaller(5 * time.Second)
	breakers := reliability.NewBreakerRegistry(5, 30*time.Second)
	credits := flow.NewCreditController(100)
	exec := executor.New(caller, breakers, credits, caller, newTestLogger())

	t.Run("high_value_goes_to_manual", func(t *testing.T) {
		msg := &executor.Message{
			Body:         []byte(`{"amount":15000}`),
			LastResponse: []byte(`{"amount":15000}`),
		}
		err := exec.Execute(context.Background(), msg, plan)
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}
	})

	t.Run("low_value_goes_to_auto", func(t *testing.T) {
		msg := &executor.Message{
			Body:         []byte(`{"amount":5000}`),
			LastResponse: []byte(`{"amount":5000}`),
		}
		err := exec.Execute(context.Background(), msg, plan)
		if err != nil {
			t.Fatalf("Execute error: %v", err)
		}
	})
}

func TestRuleMatching(t *testing.T) {
	yaml := `
targets:
  handler: "http://svc:8080/handle"
rules:
  - id: "blocked"
    priority: 0
    match:
      field: "status"
      op: "eq"
      value: "blocked"
    pipeline: "d"
  - id: "order"
    priority: 1
    match:
      field: "type"
      op: "eq"
      value: "ORDER"
    pipeline: "n:handler"
  - id: "default"
    priority: 999
    match:
      field: "*"
    pipeline: "n:handler"
`

	table, err := engine.LoadRulesBytes([]byte(yaml), 1)
	if err != nil {
		t.Fatalf("LoadRulesBytes error: %v", err)
	}

	if table.Version() != 1 {
		t.Errorf("expected version 1, got %d", table.Version())
	}

	eng := engine.New(engine.EngineConfig{})
	eng.Reload(table)

	match := eng.Match([]byte(`{"status":"blocked"}`))
	if match == nil || match.ID != "blocked" {
		t.Errorf("expected blocked rule, got %v", match)
	}

	match = eng.Match([]byte(`{"type":"ORDER"}`))
	if match == nil || match.ID != "order" {
		t.Errorf("expected order rule, got %v", match)
	}

	match = eng.Match([]byte(`{"unknown":"data"}`))
	if match == nil || match.ID != "default" {
		t.Errorf("expected default rule, got %v", match)
	}
}

func TestIdempotency(t *testing.T) {
	store := engine.NewIdempotencyStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.StartCleanup(ctx)

	key := "unique-key"
	ttl := 100 * time.Millisecond

	if store.CheckAndSet(key, ttl) {
		t.Error("expected first call to return false (not duplicate)")
	}
	if !store.CheckAndSet(key, ttl) {
		t.Error("expected second call to return true (duplicate)")
	}

	time.Sleep(150 * time.Millisecond)
	if store.CheckAndSet(key, ttl) {
		t.Error("expected call after TTL to return false (expired)")
	}
}

func compilePipeline(pipeline string, registry dsl.TargetRegistry, ruleID string, version int64) (*dsl.ExecutionPlan, error) {
	tokens, err := dsl.Lex(pipeline)
	if err != nil {
		return nil, fmt.Errorf("lex: %w", err)
	}
	instrs, err := dsl.Parse(tokens)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	optimized, err := dsl.OptimizeAndVerify(instrs)
	if err != nil {
		return nil, fmt.Errorf("optimize: %w", err)
	}
	return dsl.Compile(optimized, registry, ruleID, version)
}
