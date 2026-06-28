package dsl

import (
	"testing"
)

func TestOptimizeMergeEmit(t *testing.T) {
	instrs := []Instruction{
		{Op: OpEmit, Targets: []string{"email"}},
		{Op: OpEmit, Targets: []string{"sms"}},
		{Op: OpEmit, Targets: []string{"push"}},
	}
	result, err := Optimize(instrs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 merged emit, got %d", len(result))
	}
	if result[0].Op != OpEmit {
		t.Fatalf("expected OpEmit, got %v", result[0].Op)
	}
	if len(result[0].Targets) != 3 {
		t.Fatalf("expected 3 targets, got %d: %v", len(result[0].Targets), result[0].Targets)
	}
}

func TestOptimizeHoistTimeout(t *testing.T) {
	instrs := []Instruction{
		{Op: OpNext, TimeoutMs: 500}, // t500 placeholder
		{Op: OpNext, Targets: []string{"validate"}},
	}
	result, err := Optimize(instrs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 instruction after hoist, got %d", len(result))
	}
	if result[0].Op != OpNext {
		t.Fatalf("expected OpNext, got %v", result[0].Op)
	}
	if result[0].TimeoutMs != 500 {
		t.Fatalf("expected TimeoutMs=500, got %d", result[0].TimeoutMs)
	}
}

func TestOptimizeHoistTimeoutToParallel(t *testing.T) {
	instrs := []Instruction{
		{Op: OpNext, TimeoutMs: 1000},
		{Op: OpParallel, Targets: []string{"a", "b"}},
	}
	result, err := Optimize(instrs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 instruction after hoist, got %d", len(result))
	}
	if result[0].TimeoutMs != 1000 {
		t.Fatalf("expected TimeoutMs=1000, got %d", result[0].TimeoutMs)
	}
}

func TestOptimizeHoistRetry(t *testing.T) {
	instrs := []Instruction{
		{Op: OpNext, Targets: []string{"svc"}},
		{Op: OpNext, RetryN: 3}, // r3 placeholder
	}
	result, err := Optimize(instrs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 instruction after hoist, got %d", len(result))
	}
	if result[0].RetryN != 3 {
		t.Fatalf("expected RetryN=3, got %d", result[0].RetryN)
	}
}

func TestOptimizeRemoveAfterDrop(t *testing.T) {
	instrs := []Instruction{
		{Op: OpDrop},
		{Op: OpNext, Targets: []string{"should-not-exist"}},
	}
	result, err := Optimize(instrs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 instruction after drop removal, got %d", len(result))
	}
	if result[0].Op != OpDrop {
		t.Fatalf("expected OpDrop, got %v", result[0].Op)
	}
}

func TestOptimizeFullPipeline(t *testing.T) {
	instrs := []Instruction{
		{Op: OpNext, TimeoutMs: 500},
		{Op: OpNext, Targets: []string{"validate"}},
		{Op: OpNext, TimeoutMs: 1000},
		{Op: OpParallel, Targets: []string{"fraud", "inventory"}},
		{Op: OpCollect},
		{Op: OpFallback, Targets: []string{"dlq"}},
		{Op: OpNext, Targets: []string{"fulfill"}},
		{Op: OpEmit, Targets: []string{"notify", "analytics"}},
	}
	result, err := Optimize(instrs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// After optimization:
	// n:validate{TimeoutMs:500}, p:fraud,inventory{TimeoutMs:1000}, c, f:dlq, n:fulfill, e:notify,analytics
	if len(result) != 6 {
		t.Fatalf("expected 6 instructions after optimization, got %d", len(result))
	}
	if result[0].Op != OpNext || result[0].TimeoutMs != 500 || result[0].Targets[0] != "validate" {
		t.Errorf("result[0] = %+v", result[0])
	}
	if result[1].Op != OpParallel || result[1].TimeoutMs != 1000 {
		t.Errorf("result[1] = %+v", result[1])
	}
	if result[2].Op != OpCollect {
		t.Errorf("result[2] = %+v", result[2])
	}
	if result[3].Op != OpFallback {
		t.Errorf("result[3] = %+v", result[3])
	}
	if result[4].Op != OpNext || result[4].Targets[0] != "fulfill" {
		t.Errorf("result[4] = %+v", result[4])
	}
	if result[5].Op != OpEmit {
		t.Errorf("result[5] = %+v", result[5])
	}
}

func TestOptimizeNoOpTimeoutInOutput(t *testing.T) {
	instrs := []Instruction{
		{Op: OpNext, TimeoutMs: 500},
		{Op: OpNext, Targets: []string{"svc"}},
	}
	result, err := OptimizeAndVerify(instrs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, instr := range result {
		if instr.TimeoutMs > 0 && len(instr.Targets) == 0 && instr.Op == OpNext {
			t.Errorf("orphaned timeout instruction: %+v", instr)
		}
	}
}

func TestOptimizeNoOpRetryInOutput(t *testing.T) {
	instrs := []Instruction{
		{Op: OpNext, Targets: []string{"svc"}},
		{Op: OpNext, RetryN: 3},
	}
	result, err := OptimizeAndVerify(instrs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, instr := range result {
		if instr.RetryN > 0 && len(instr.Targets) == 0 && instr.Op == OpNext {
			t.Errorf("orphaned retry instruction: %+v", instr)
		}
	}
}
