package flow

import (
	"context"
	"testing"
)

func TestStartFlow(t *testing.T) {
	o := NewOrchestrator()
	f := o.Start(context.Background(), "flow-1", "rule-1", []byte("input"))
	if f == nil {
		t.Fatal("expected non-nil flow")
	}
	if f.State != StatePending {
		t.Errorf("expected StatePending, got %v", f.State)
	}
	if f.ID != "flow-1" {
		t.Errorf("expected flow-1, got %s", f.ID)
	}
	if string(f.Input) != "input" {
		t.Errorf("expected 'input', got %s", f.Input)
	}
}

func TestGetFlow(t *testing.T) {
	o := NewOrchestrator()
	o.Start(context.Background(), "flow-1", "rule-1", nil)

	f, ok := o.Get("flow-1")
	if !ok {
		t.Fatal("expected to find flow-1")
	}
	if f.RuleID != "rule-1" {
		t.Errorf("expected rule-1, got %s", f.RuleID)
	}

	_, ok = o.Get("nonexistent")
	if ok {
		t.Error("expected false for nonexistent flow")
	}
}

func TestStoreResponse(t *testing.T) {
	o := NewOrchestrator()
	o.Start(context.Background(), "flow-1", "rule-1", nil)
	o.StoreResponse("flow-1", 42, []byte("response"))

	f, _ := o.Get("flow-1")
	f.mu.Lock()
	resp, ok := f.responses[42]
	f.mu.Unlock()
	if !ok {
		t.Fatal("expected response for service 42")
	}
	if string(resp) != "response" {
		t.Errorf("expected 'response', got %s", resp)
	}
}

func TestStoreResponseNonexistentFlow(t *testing.T) {
	o := NewOrchestrator()
	o.StoreResponse("ghost", 1, []byte("data"))
}
