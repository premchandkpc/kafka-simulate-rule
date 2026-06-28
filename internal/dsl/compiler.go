package dsl

import (
	"fmt"
	"strconv"
	"strings"
)

// TargetRegistry maps target names to addresses.
type TargetRegistry map[string]string

// ExecutionPlan is the compiled, immutable result of DSL compilation.
type ExecutionPlan struct {
	RuleID       string
	Instructions []Instruction
	Version      int64
}

// Compile resolves target names, assigns indices, and produces the final plan.
func Compile(instrs []Instruction, registry TargetRegistry, ruleID string, version int64) (*ExecutionPlan, error) {
	compiled := make([]Instruction, 0, len(instrs))

	for i, instr := range instrs {
		cp := instr

		switch instr.Op {
		case OpNext, OpFallback:
			resolved, err := resolveTargets(instr.Targets, registry, i)
			if err != nil {
				return nil, err
			}
			cp.Targets = resolved

		case OpEmit:
			resolved, err := resolveTargets(instr.Targets, registry, i)
			if err != nil {
				return nil, err
			}
			cp.Targets = resolved

		case OpParallel:
			resolved, err := resolveTargets(instr.Targets, registry, i)
			if err != nil {
				return nil, err
			}
			cp.Targets = resolved

		case OpGate:
			if err := validateGateOperand(instr.Operand, i); err != nil {
				return nil, err
			}

		case OpKey, OpSplit:
			if err := validateKeyField(instr.Operand, i); err != nil {
				return nil, err
			}

		case OpBuffer:
			if cp.RetryN <= 0 || cp.RetryN > 10000 {
				return nil, fmt.Errorf("compiler: invalid buffer count %d at index %d", cp.RetryN, i)
			}
			cp.Operand = strconv.Itoa(cp.RetryN)

		case OpMap:
			if cp.MapExpr == nil {
				return nil, fmt.Errorf("compiler: OpMap with nil MapExpr at index %d", i)
			}

		case OpDrop, OpCollect, OpPipe:
			// No targets to resolve
		}

		compiled = append(compiled, cp)
	}

	return &ExecutionPlan{
		RuleID:       ruleID,
		Instructions: compiled,
		Version:      version,
	}, nil
}

func resolveTargets(targets []string, registry TargetRegistry, idx int) ([]string, error) {
	resolved := make([]string, len(targets))
	for i, t := range targets {
		t = strings.TrimSpace(t)
		if t == "" {
			return nil, fmt.Errorf("compiler: empty target at index %d", idx)
		}
		addr, ok := registry[t]
		if !ok {
			return nil, fmt.Errorf("compiler: target %q not found in registry at index %d", t, idx)
		}
		resolved[i] = addr
	}
	return resolved, nil
}

func validateGateOperand(operand string, idx int) error {
	if operand == "" {
		return fmt.Errorf("compiler: empty gate field at index %d", idx)
	}
	return nil
}

func validateKeyField(field string, idx int) error {
	if field == "" {
		return fmt.Errorf("compiler: empty key/split field at index %d", idx)
	}
	return nil
}
