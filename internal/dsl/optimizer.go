package dsl

import (
	"fmt"
	"log"
)

// Optimize transforms the raw instruction stream into optimized bytecode.
// Transformations:
//  1. Merge consecutive e: into one e: with combined targets
//  2. Remove instructions after d (unreachable)
//  3. Hoist t<ms> into the TimeoutMs field of the NEXT instruction, then remove OpTimeout
//  4. Hoist r<n> into the RetryN field of the PRECEDING n: instruction, then remove OpRetry
func Optimize(instrs []Instruction) ([]Instruction, error) {
	instrs = mergeEmit(instrs)
	instrs = removeAfterDrop(instrs)
	instrs = hoistTimeout(instrs)
	instrs = hoistRetry(instrs)
	return instrs, nil
}

// mergeEmit combines consecutive e: instructions into one.
func mergeEmit(instrs []Instruction) []Instruction {
	if len(instrs) == 0 {
		return instrs
	}

	var result []Instruction
	i := 0
	for i < len(instrs) {
		if instrs[i].Op == OpEmit {
			merged := instrs[i]
			j := i + 1
			for j < len(instrs) && instrs[j].Op == OpEmit {
				merged.Targets = append(merged.Targets, instrs[j].Targets...)
				j++
			}
			result = append(result, merged)
			i = j
		} else {
			result = append(result, instrs[i])
			i++
		}
	}
	return result
}

// removeAfterDrop removes all instructions after a d (drop).
func removeAfterDrop(instrs []Instruction) []Instruction {
	for i, instr := range instrs {
		if instr.Op == OpDrop {
			if i+1 < len(instrs) {
				log.Printf("dsl: warning — removing %d unreachable instruction(s) after d", len(instrs)-i-1)
			}
			return instrs[:i+1]
		}
	}
	return instrs
}

// hoistTimeout moves t<ms> values into the TimeoutMs field of the next instruction,
// then removes the timeout placeholder instruction.
// After this, no OpNext with TimeoutMs==0 remains from a timeout placeholder.
func hoistTimeout(instrs []Instruction) []Instruction {
	var result []Instruction
	var pendingTimeout int64

	for _, instr := range instrs {
		if instr.TimeoutMs > 0 && len(instr.Targets) == 0 && instr.Op == OpNext {
			// This is a timeout placeholder from the parser (no targets, only TimeoutMs set)
			pendingTimeout = instr.TimeoutMs
			continue
		}
		if pendingTimeout > 0 {
			if instr.Op == OpNext || instr.Op == OpParallel {
				instr.TimeoutMs = pendingTimeout
				pendingTimeout = 0
			}
			// If instruction doesn't support timeout (like OpPipe, OpGate),
			// we keep the timeout for the next applicable instruction.
			// For non-applicable instructions, we warn and drop the timeout.
		}
		if pendingTimeout > 0 && instr.Op != OpNext && instr.Op != OpParallel {
			// t before a non-applicable instruction — this is unreachable timeout
			// It might have applied to a previous instruction; if there's no pending
			// applicable instruction ahead, we drop it.
			if !isTimeoutApplicable(instr.Op) {
				log.Printf("dsl: warning — timeout before %s, dropping", instr.Op)
				pendingTimeout = 0
			}
		}
		result = append(result, instr)
	}

	if pendingTimeout > 0 {
		log.Printf("dsl: warning — trailing timeout %d with no target, dropping", pendingTimeout)
	}

	return result
}

func isTimeoutApplicable(op OpCode) bool {
	return op == OpNext || op == OpParallel || op == OpCollect
}

// hoistRetry moves r<n> into the RetryN field of the preceding n: instruction.
// r placeholders have Op==OpNext, RetryN>0, and empty Targets.
func hoistRetry(instrs []Instruction) []Instruction {
	var result []Instruction

	for i := 0; i < len(instrs); i++ {
		instr := instrs[i]

		if instr.RetryN > 0 && len(instr.Targets) == 0 && instr.Op == OpNext {
			// This is a retry placeholder — apply to the previous n:
			if len(result) > 0 && result[len(result)-1].Op == OpNext {
				result[len(result)-1].RetryN = instr.RetryN
			} else {
				log.Printf("dsl: warning — retry without preceding next, dropping")
			}
			continue
		}

		result = append(result, instr)
	}

	return result
}

// OptimizeAndVerify runs the optimizer and validates the output.
func OptimizeAndVerify(instrs []Instruction) ([]Instruction, error) {
	result, err := Optimize(instrs)
	if err != nil {
		return nil, err
	}

	// Verify no timeout or retry placeholders remain
	for _, instr := range result {
		if instr.TimeoutMs > 0 && len(instr.Targets) == 0 && instr.Op == OpNext {
			return nil, fmt.Errorf("optimizer: orphaned timeout")
		}
		if instr.RetryN > 0 && len(instr.Targets) == 0 && instr.Op == OpNext {
			return nil, fmt.Errorf("optimizer: orphaned retry")
		}
	}

	return result, nil
}
