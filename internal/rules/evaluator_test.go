package rules

import (
	"encoding/json"
	"testing"

	"github.com/flowrule/flowrule/internal/domain"
)

func TestEvaluateSimpleEq(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
		"rule_set": "order-policy",
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
					"data": {"order_id": "$.id"}
				}
			}]
		}]
	}`)

	revision, err := compiler.Compile(source)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	evaluator := NewEvaluator()
	event := &domain.EventEnvelope{
		ID:           "evt-1",
		Type:         "order.created",
		TenantID:     "acme",
		PartitionKey: "order-1",
		Data:         json.RawMessage(`{"total": 1500, "id": "order-1"}`),
	}

	decision, err := evaluator.Evaluate(revision, event, nil)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	if len(decision.MatchedRules) != 1 {
		t.Errorf("expected 1 matched rule, got %d", len(decision.MatchedRules))
	}
	if len(decision.Effects) != 1 {
		t.Errorf("expected 1 effect, got %d", len(decision.Effects))
	}
	if decision.Hash == "" {
		t.Error("expected non-empty hash")
	}
}

func TestEvaluateNoMatch(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
		"rule_set": "order-policy",
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
					"data": {"order_id": "$.id"}
				}
			}]
		}]
	}`)

	revision, err := compiler.Compile(source)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	evaluator := NewEvaluator()
	event := &domain.EventEnvelope{
		ID:           "evt-2",
		Type:         "order.created",
		TenantID:     "acme",
		PartitionKey: "order-2",
		Data:         json.RawMessage(`{"total": 500, "id": "order-2"}`),
	}

	decision, err := evaluator.Evaluate(revision, event, nil)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	if len(decision.MatchedRules) != 0 {
		t.Errorf("expected 0 matched rules, got %d", len(decision.MatchedRules))
	}
	if len(decision.Effects) != 0 {
		t.Errorf("expected 0 effects, got %d", len(decision.Effects))
	}
}

func TestEvaluateAllMatches(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
		"rule_set": "order-policy",
		"revision": 1,
		"mode": "all_matches",
		"rules": [
			{
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
			},
			{
				"id": "us-order",
				"priority": 50,
				"when": {
					"path": "$.country",
					"op": "eq",
					"value": "US"
				},
				"then": [{
					"emit": {
						"topic": "orders.us",
						"data": {"reason": "us_order"}
					}
				}]
			}
		]
	}`)

	revision, err := compiler.Compile(source)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	evaluator := NewEvaluator()
	event := &domain.EventEnvelope{
		ID:           "evt-3",
		Type:         "order.created",
		TenantID:     "acme",
		PartitionKey: "order-3",
		Data:         json.RawMessage(`{"total": 1500, "country": "US"}`),
	}

	decision, err := evaluator.Evaluate(revision, event, nil)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	if len(decision.MatchedRules) != 2 {
		t.Errorf("expected 2 matched rules, got %d", len(decision.MatchedRules))
	}
	if len(decision.Effects) != 2 {
		t.Errorf("expected 2 effects, got %d", len(decision.Effects))
	}
}

func TestEvaluateDedeterministicHash(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
		"rule_set": "order-policy",
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
					"data": {"order_id": "$.id"}
				}
			}]
		}]
	}`)

	revision, err := compiler.Compile(source)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	evaluator := NewEvaluator()
	event := &domain.EventEnvelope{
		ID:           "evt-4",
		Type:         "order.created",
		TenantID:     "acme",
		PartitionKey: "order-4",
		Data:         json.RawMessage(`{"total": 1500, "id": "order-4"}`),
	}

	decision1, _ := evaluator.Evaluate(revision, event, nil)
	decision2, _ := evaluator.Evaluate(revision, event, nil)

	if decision1.Hash != decision2.Hash {
		t.Errorf("expected deterministic hash: %s != %s", decision1.Hash, decision2.Hash)
	}
}

func TestCompilerInvalidMatchMode(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
		"rule_set": "order-policy",
		"revision": 1,
		"mode": "invalid",
		"rules": [{
			"id": "r1",
			"priority": 1,
			"when": {"path": "$.x", "op": "eq", "value": 1},
			"then": [{"emit": {"topic": "t", "data": {}}}]
		}]
	}`)

	_, err := compiler.Compile(source)
	if err == nil {
		t.Fatal("expected error for invalid match mode")
	}
}

func TestCompilerDuplicateRuleIDs(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
		"rule_set": "order-policy",
		"revision": 1,
		"mode": "first_match",
		"rules": [
			{
				"id": "same-id",
				"priority": 1,
				"when": {"path": "$.x", "op": "eq", "value": 1},
				"then": [{"emit": {"topic": "t", "data": {}}}]
			},
			{
				"id": "same-id",
				"priority": 2,
				"when": {"path": "$.x", "op": "eq", "value": 2},
				"then": [{"emit": {"topic": "t", "data": {}}}]
			}
		]
	}`)

	_, err := compiler.Compile(source)
	if err == nil {
		t.Fatal("expected error for duplicate rule IDs")
	}
}

func TestCompilerAmbiguousPriority(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
		"rule_set": "order-policy",
		"revision": 1,
		"mode": "first_match",
		"rules": [
			{
				"id": "r1",
				"priority": 1,
				"when": {"path": "$.x", "op": "eq", "value": 1},
				"then": [{"emit": {"topic": "t", "data": {}}}]
			},
			{
				"id": "r2",
				"priority": 1,
				"when": {"path": "$.x", "op": "eq", "value": 2},
				"then": [{"emit": {"topic": "t", "data": {}}}]
			}
		]
	}`)

	_, err := compiler.Compile(source)
	if err == nil {
		t.Fatal("expected error for ambiguous priority")
	}
}

func TestCompilerRejectsEarlyOtherwiseInFirstMatch(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
		"rule_set": "order-policy",
		"revision": 1,
		"mode": "first_match",
		"rules": [
			{
				"id": "high-priority", "priority": 100,
				"when": {"path": "$.x", "op": "eq", "value": 1},
				"then": [{"emit": {"topic": "matched", "data": {}}}],
				"otherwise": [{"emit": {"topic": "unexpected", "data": {}}}]
			},
			{
				"id": "fallback", "priority": 1,
				"when": {"path": "$.x", "op": "eq", "value": 2},
				"then": [{"emit": {"topic": "fallback-match", "data": {}}}],
				"otherwise": [{"emit": {"topic": "fallback", "data": {}}}]
			}
		]
	}`)

	_, err := compiler.Compile(source)
	if err == nil {
		t.Fatal("expected first_match rule set with an early otherwise to be rejected")
	}
}
