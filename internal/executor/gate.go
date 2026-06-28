package executor

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/premchand/flowrule/internal/dsl"
)

// evalGate extracts a field from msg.LastResponse and compares it.
func evalGate(msg *Message, instr dsl.Instruction) (bool, error) {
	body := msg.LastResponse
	if len(body) == 0 {
		body = msg.Body
	}

	val, err := extractJSONFieldString(body, strings.Split(instr.Operand, "."))
	if err != nil {
		return false, fmt.Errorf("gate: field %q: %w", instr.Operand, err)
	}

	return compareValues(val, instr.Operator, instr.Value)
}

func extractJSONFieldString(body []byte, path []string) (string, error) {
	var data any
	if err := json.Unmarshal(body, &data); err != nil {
		return "", fmt.Errorf("unmarshal: %w", err)
	}

	current := data
	for _, part := range path {
		m, ok := current.(map[string]any)
		if !ok {
			return "", fmt.Errorf("field %q not found in non-object", part)
		}
		val, ok := m[part]
		if !ok {
			return "", fmt.Errorf("field %q not found", part)
		}
		current = val
	}

	// Convert to string representation
	switch v := current.(type) {
	case string:
		return v, nil
	case float64:
		if v == math.Trunc(v) {
			return strconv.FormatInt(int64(v), 10), nil
		}
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(v), nil
	case nil:
		return "null", nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("marshal value: %w", err)
		}
		return string(b), nil
	}
}

func compareValues(val, operator, target string) (bool, error) {
	switch operator {
	case "==":
		return val == target, nil
	case "!=":
		return val != target, nil
	case "contains":
		return strings.Contains(val, target), nil
	case ">", "<", ">=", "<=":
		fVal, err1 := strconv.ParseFloat(val, 64)
		fTarget, err2 := strconv.ParseFloat(target, 64)
		if err1 != nil || err2 != nil {
			return false, fmt.Errorf("gate: numeric comparison requires numeric fields, got %q vs %q", val, target)
		}
		switch operator {
		case ">":
			return fVal > fTarget, nil
		case "<":
			return fVal < fTarget, nil
		case ">=":
			return fVal >= fTarget, nil
		case "<=":
			return fVal <= fTarget, nil
		}
	}
	return false, fmt.Errorf("gate: unknown operator %q", operator)
}

// skipToPipe advances index i to the next OpPipe instruction.
func skipToPipe(instrs []dsl.Instruction, i int) int {
	for j := i + 1; j < len(instrs); j++ {
		if instrs[j].Op == dsl.OpPipe {
			return j
		}
	}
	return len(instrs) - 1
}

// skipToEnd skips to the instruction after the next OpPipe.
func skipToEnd(instrs []dsl.Instruction, i int) int {
	for j := i + 1; j < len(instrs); j++ {
		if instrs[j].Op == dsl.OpPipe {
			return j
		}
	}
	return len(instrs) - 1
}
