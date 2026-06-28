package dsl

import (
	"testing"
)

func TestCompile(t *testing.T) {
	registry := TargetRegistry{
		"validate":  "http://validate:8080",
		"fraud":     "http://fraud:8080",
		"inventory": "http://inventory:8080",
		"dlq":       "http://dlq:8080",
		"fulfill":   "http://fulfill:8080",
		"notify":    "http://notify:8080",
		"analytics": "http://analytics:8080",
	}

	instrs := []Instruction{
		{Op: OpNext, Targets: []string{"validate"}, TimeoutMs: 500},
		{Op: OpParallel, Targets: []string{"fraud", "inventory"}},
		{Op: OpCollect},
		{Op: OpFallback, Targets: []string{"dlq"}},
		{Op: OpNext, Targets: []string{"fulfill"}},
		{Op: OpEmit, Targets: []string{"notify", "analytics"}},
	}

	plan, err := Compile(instrs, registry, "test-rule", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.RuleID != "test-rule" {
		t.Errorf("RuleID = %q, want %q", plan.RuleID, "test-rule")
	}
	if plan.Version != 1 {
		t.Errorf("Version = %d, want 1", plan.Version)
	}
	if len(plan.Instructions) != len(instrs) {
		t.Errorf("Instructions len = %d, want %d", len(plan.Instructions), len(instrs))
	}
}

func TestCompileUnknownTarget(t *testing.T) {
	registry := TargetRegistry{
		"validate": "http://validate:8080",
	}

	instrs := []Instruction{
		{Op: OpNext, Targets: []string{"unknown-svc"}},
	}

	_, err := Compile(instrs, registry, "test-rule", 1)
	if err == nil {
		t.Fatal("expected error for unknown target")
	}
}

func TestCompileEmptyTarget(t *testing.T) {
	registry := TargetRegistry{
		"validate": "http://validate:8080",
	}

	instrs := []Instruction{
		{Op: OpNext, Targets: []string{""}},
	}

	_, err := Compile(instrs, registry, "test-rule", 1)
	if err == nil {
		t.Fatal("expected error for empty target")
	}
}

func TestCompileMapExpr(t *testing.T) {
	registry := TargetRegistry{}

	instrs := []Instruction{
		{Op: OpMap, MapExpr: &MapExpr{FieldPath: []string{"result"}}},
	}

	plan, err := Compile(instrs, registry, "test-map", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(plan.Instructions) != 1 {
		t.Fatalf("expected 1 instruction, got %d", len(plan.Instructions))
	}
	if plan.Instructions[0].MapExpr == nil {
		t.Fatal("MapExpr should not be nil")
	}
}

func TestCompileNilMapExpr(t *testing.T) {
	registry := TargetRegistry{}

	instrs := []Instruction{
		{Op: OpMap},
	}

	_, err := Compile(instrs, registry, "test-map", 1)
	if err == nil {
		t.Fatal("expected error for nil MapExpr")
	}
}

func TestCompileEndToEnd(t *testing.T) {
	input := "t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics"
	tokens, err := Lex(input)
	if err != nil {
		t.Fatalf("Lex error: %v", err)
	}
	instrs, err := Parse(tokens)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	optimized, err := OptimizeAndVerify(instrs)
	if err != nil {
		t.Fatalf("Optimize error: %v", err)
	}

	registry := TargetRegistry{
		"validate":  "http://validate:8080",
		"fraud":     "http://fraud:8080",
		"inventory": "http://inventory:8080",
		"dlq":       "http://dlq:8080",
		"fulfill":   "http://fulfill:8080",
		"notify":    "http://notify:8080",
		"analytics": "http://analytics:8080",
	}

	plan, err := Compile(optimized, registry, "order-pipeline", 1)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	if len(plan.Instructions) != 6 {
		t.Fatalf("expected 6 instructions, got %d", len(plan.Instructions))
	}

	// n:validate {TimeoutMs:500}
	if plan.Instructions[0].Op != OpNext || plan.Instructions[0].TimeoutMs != 500 {
		t.Errorf("instr[0] = %+v", plan.Instructions[0])
	}
	// p:fraud,inventory {TimeoutMs:1000}
	if plan.Instructions[1].Op != OpParallel || plan.Instructions[1].TimeoutMs != 1000 {
		t.Errorf("instr[1] = %+v", plan.Instructions[1])
	}
	// c
	if plan.Instructions[2].Op != OpCollect {
		t.Errorf("instr[2] = %+v", plan.Instructions[2])
	}
	// f:dlq
	if plan.Instructions[3].Op != OpFallback || plan.Instructions[3].Targets[0] != "http://dlq:8080" {
		t.Errorf("instr[3] = %+v", plan.Instructions[3])
	}
	// n:fulfill
	if plan.Instructions[4].Op != OpNext || plan.Instructions[4].Targets[0] != "http://fulfill:8080" {
		t.Errorf("instr[4] = %+v", plan.Instructions[4])
	}
	// e:notify,analytics
	if plan.Instructions[5].Op != OpEmit || len(plan.Instructions[5].Targets) != 2 {
		t.Errorf("instr[5] = %+v", plan.Instructions[5])
	}
	if plan.Instructions[5].Targets[0] != "http://notify:8080" {
		t.Errorf("instr[5].Targets[0] = %q, want %q", plan.Instructions[5].Targets[0], "http://notify:8080")
	}
	if plan.Instructions[5].Targets[1] != "http://analytics:8080" {
		t.Errorf("instr[5].Targets[1] = %q, want %q", plan.Instructions[5].Targets[1], "http://analytics:8080")
	}
}
