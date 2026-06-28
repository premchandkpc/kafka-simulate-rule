package flow

import (
	"context"
	"sync"
)

type FlowState int

const (
	StatePending FlowState = iota
	StateRunning
	StateCompleted
	StateFailed
)

type Flow struct {
	ID        string
	RuleID    string
	Input     []byte
	Output    []byte
	State     FlowState
	Error     string
	responses map[uint16][]byte
	mu        sync.Mutex
}

type Orchestrator struct {
	mu    sync.Mutex
	flows map[string]*Flow
}

func NewOrchestrator() *Orchestrator {
	return &Orchestrator{flows: make(map[string]*Flow)}
}

func (o *Orchestrator) Start(ctx context.Context, id, ruleID string, input []byte) *Flow {
	f := &Flow{
		ID:        id,
		RuleID:    ruleID,
		Input:     input,
		State:     StatePending,
		responses: make(map[uint16][]byte),
	}
	o.mu.Lock()
	o.flows[id] = f
	o.mu.Unlock()
	return f
}

func (o *Orchestrator) Get(id string) (*Flow, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	f, ok := o.flows[id]
	return f, ok
}

func (o *Orchestrator) StoreResponse(flowID string, svcID uint16, resp []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if f, ok := o.flows[flowID]; ok {
		f.mu.Lock()
		f.responses[svcID] = resp
		f.mu.Unlock()
	}
}
