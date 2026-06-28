package engine

import (
	"sync"

	"github.com/premchandkpc/kafka-simulate-rule/go/internal/bridge"
)

type Rule struct {
	ID       string
	DSL      string
	Plan     []byte
	Priority int
}

type Engine struct {
	mu    sync.RWMutex
	rules []Rule
}

func New() *Engine {
	return &Engine{}
}

func (e *Engine) Deploy(id, dsl string) error {
	plan, err := bridge.Compile(dsl, id)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = append(e.rules, Rule{ID: id, DSL: dsl, Plan: plan})
	return nil
}

func (e *Engine) Remove(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, r := range e.rules {
		if r.ID == id {
			e.rules = append(e.rules[:i], e.rules[i+1:]...)
			return
		}
	}
}

func (e *Engine) Rules() []Rule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]Rule, len(e.rules))
	copy(out, e.rules)
	return out
}

func (e *Engine) ExecuteAll(body []byte, caller bridge.ServiceCaller) ([][]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var results [][]byte
	for _, r := range e.rules {
		res, err := bridge.Execute(r.Plan, body, caller)
		if err != nil {
			return results, err
		}
		results = append(results, res)
	}
	return results, nil
}
