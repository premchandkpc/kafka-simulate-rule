package engine

import (
	"testing"
)

func TestNewEngine(t *testing.T) {
	e := New()
	if e == nil {
		t.Fatal("expected non-nil engine")
	}
}

func TestDeployCompile(t *testing.T) {
	e := New()
	err := e.Deploy("test-1", "n:validate")
	if err != nil {
		t.Fatalf("Deploy failed: %v", err)
	}
}

func TestDeployInvalidDSL(t *testing.T) {
	e := New()
	err := e.Deploy("bad-rule", "invalid!!!dsl")
	if err == nil {
		t.Fatal("expected error for invalid DSL")
	}
}

func TestRemoveRule(t *testing.T) {
	e := New()
	e.Deploy("test-1", "n:validate")
	e.Deploy("test-2", "n:validate")

	e.Remove("test-1")
	rules := e.Rules()
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	if rules[0].ID != "test-2" {
		t.Errorf("expected test-2, got %s", rules[0].ID)
	}
}

func TestExecuteAll(t *testing.T) {
	e := New()
	e.Deploy("test-1", "n:validate")

	results, err := e.ExecuteAll([]byte(`{}`), nil)
	if err != nil {
		t.Fatalf("ExecuteAll failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}
