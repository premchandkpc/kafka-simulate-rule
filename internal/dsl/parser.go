package dsl

import (
	"fmt"
	"strconv"
	"strings"
)

type OpCode uint8

const (
	OpNext     OpCode = iota // n:
	OpParallel               // p:
	OpCollect                // c
	OpFallback               // f:
	OpGate                   // g:
	OpPipe                   // | — branch separator
	OpSplit                  // s:
	OpMap                    // m:
	OpEmit                   // e:
	OpDrop                   // d
	OpBuffer                 // b
	OpKey                    // k:
)

func (o OpCode) String() string {
	switch o {
	case OpNext:
		return "next"
	case OpParallel:
		return "parallel"
	case OpCollect:
		return "collect"
	case OpFallback:
		return "fallback"
	case OpGate:
		return "gate"
	case OpPipe:
		return "pipe"
	case OpSplit:
		return "split"
	case OpMap:
		return "map"
	case OpEmit:
		return "emit"
	case OpDrop:
		return "drop"
	case OpBuffer:
		return "buffer"
	case OpKey:
		return "key"
	default:
		return fmt.Sprintf("opcode(%d)", o)
	}
}

type Instruction struct {
	Op        OpCode
	Targets   []string
	Operand   string
	Operator  string
	Value     string
	TimeoutMs int64
	RetryN    int
	MapExpr   *MapExpr
}

type ParseError struct {
	Token string
	Msg   string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("dsl: parse error at %q: %s", e.Token, e.Msg)
}

// Parse converts a token stream into validated instructions.
func Parse(tokens []Token) ([]Instruction, error) {
	var instrs []Instruction
	parallelOpen := false
	gateSincePipe := false
	lastWasCollect := false

	for _, tok := range tokens {
		switch tok.Type {
		case TokenTimeout:
			ms, _ := strconv.ParseInt(tok.Raw[1:], 10, 64)
			if ms <= 0 {
				return nil, &ParseError{Token: tok.Raw, Msg: "timeout must be positive"}
			}
			instrs = append(instrs, Instruction{Op: OpNext, TimeoutMs: ms}) // placeholder; optimizer hoists

		case TokenNext:
			target := tok.Raw[2:]
			if strings.Contains(target, ",") {
				return nil, &ParseError{Token: tok.Raw, Msg: "n: must have exactly one target"}
			}
			if parallelOpen {
				return nil, &ParseError{Token: tok.Raw, Msg: "n: inside parallel block is not allowed before c"}
			}
			instrs = append(instrs, Instruction{Op: OpNext, Targets: []string{target}})

		case TokenParallel:
			targets := strings.Split(tok.Raw[2:], ",")
			if len(targets) < 2 {
				return nil, &ParseError{Token: tok.Raw, Msg: "p: must have at least two targets"}
			}
			if parallelOpen {
				return nil, &ParseError{Token: tok.Raw, Msg: "nested parallel blocks not allowed"}
			}
			parallelOpen = true
			lastWasCollect = false
			instrs = append(instrs, Instruction{Op: OpParallel, Targets: targets})

		case TokenCollect:
			if !parallelOpen {
				return nil, &ParseError{Token: tok.Raw, Msg: "c without preceding p:"}
			}
			parallelOpen = false
			lastWasCollect = true
			instrs = append(instrs, Instruction{Op: OpCollect})

		case TokenRetry:
			if lastWasCollect {
				return nil, &ParseError{Token: tok.Raw, Msg: "r after c is not allowed (r only applies to n:)"}
			}
			n, _ := strconv.Atoi(tok.Raw[1:])
			if n <= 0 {
				return nil, &ParseError{Token: tok.Raw, Msg: "retry count must be positive"}
			}
			if n > 100 {
				return nil, &ParseError{Token: tok.Raw, Msg: "retry count exceeds max (100)"}
			}
			// Check that the immediately preceding instruction is an n:
			if len(instrs) == 0 {
				return nil, &ParseError{Token: tok.Raw, Msg: "r without preceding instruction"}
			}
			prev := instrs[len(instrs)-1]
			if prev.Op != OpNext && prev.Op != OpCollect {
				// OpCollect also counts if there was a collect but we alread checked for that
				return nil, &ParseError{Token: tok.Raw, Msg: fmt.Sprintf("r only applies to n:, not %s", prev.Op)}
			}
			// If there's a preceding timeout placeholder, skip back over it
			// Actually, the r applies to the previous n:, so we store retry count as a placeholder
			instrs = append(instrs, Instruction{Op: OpNext, RetryN: n}) // placeholder; optimizer hoists

		case TokenFallback:
			target := tok.Raw[2:]
			if strings.Contains(target, ",") {
				return nil, &ParseError{Token: tok.Raw, Msg: "f: must have exactly one target"}
			}
			instrs = append(instrs, Instruction{Op: OpFallback, Targets: []string{target}})

		case TokenGate:
			raw := tok.Raw[2:]
			fieldPath, operator, value, err := parseGate(raw)
			if err != nil {
				return nil, &ParseError{Token: tok.Raw, Msg: err.Error()}
			}
			gateSincePipe = true
			instrs = append(instrs, Instruction{Op: OpGate, Operand: fieldPath, Operator: operator, Value: value})

		case TokenPipe:
			if !gateSincePipe {
				return nil, &ParseError{Token: tok.Raw, Msg: "| without preceding g:"}
			}
			gateSincePipe = false
			parallelOpen = false
			lastWasCollect = false
			instrs = append(instrs, Instruction{Op: OpPipe})

		case TokenSplit:
			field := tok.Raw[2:]
			if field == "" {
				return nil, &ParseError{Token: tok.Raw, Msg: "s: requires a field name"}
			}
			instrs = append(instrs, Instruction{Op: OpSplit, Operand: field})

		case TokenMap:
			expr := tok.Raw[2:]
			if expr == "" {
				return nil, &ParseError{Token: tok.Raw, Msg: "m: requires an expression"}
			}
			mapExpr, err := ParseMapExpr(expr)
			if err != nil {
				return nil, &ParseError{Token: tok.Raw, Msg: err.Error()}
			}
			instrs = append(instrs, Instruction{Op: OpMap, MapExpr: mapExpr})

		case TokenEmit:
			targets := strings.Split(tok.Raw[2:], ",")
			if len(targets) < 1 {
				return nil, &ParseError{Token: tok.Raw, Msg: "e: requires at least one target"}
			}
			if parallelOpen {
				parallelOpen = false
			}
			lastWasCollect = false
			instrs = append(instrs, Instruction{Op: OpEmit, Targets: targets})

		case TokenDrop:
			// d after d is dead code but not an error
			if len(instrs) > 0 && instrs[len(instrs)-1].Op == OpDrop {
				// dead code warning — skip duplicate d
				continue
			}
			lastWasCollect = false
			instrs = append(instrs, Instruction{Op: OpDrop})

		case TokenBuffer:
			n, _ := strconv.Atoi(tok.Raw[1:])
			if n <= 0 {
				return nil, &ParseError{Token: tok.Raw, Msg: "buffer count must be positive"}
			}
			if n > 10000 {
				return nil, &ParseError{Token: tok.Raw, Msg: "buffer count exceeds max (10000)"}
			}
			lastWasCollect = false
			instrs = append(instrs, Instruction{Op: OpBuffer, RetryN: n}) // reuse RetryN for buffer count

		case TokenKey:
			field := tok.Raw[2:]
			if field == "" {
				return nil, &ParseError{Token: tok.Raw, Msg: "k: requires a field name"}
			}
			lastWasCollect = false
			instrs = append(instrs, Instruction{Op: OpKey, Operand: field})
		}
	}

	if parallelOpen {
		return nil, fmt.Errorf("dsl: unclosed parallel block (p: without c or e:)")
	}

	return instrs, nil
}

// Gate operator prefixes in order: longest first to avoid greedy mismatch.
var gateOperators = []string{
	"contains", // "contains" must be checked before "<" to avoid partial match
	">=",
	"<=",
	"==",
	"!=",
	">",
	"<",
}

func parseGate(raw string) (fieldPath, operator, value string, err error) {
	if raw == "" {
		return "", "", "", fmt.Errorf("empty gate expression")
	}

	var opIdx int
	var opStr string
	found := false

	for _, op := range gateOperators {
		idx := strings.Index(raw, op)
		if idx >= 0 {
			if !found || idx < opIdx {
				opIdx = idx
				opStr = op
				found = true
			}
		}
	}

	if !found {
		return "", "", "", fmt.Errorf("no operator found in gate expression %q", raw)
	}

	fieldPath = raw[:opIdx]
	value = raw[opIdx+len(opStr):]
	operator = opStr

	// Strip trailing dots from field path (e.g., "tags." becomes "tags" for "contains" operator)
	fieldPath = strings.TrimRight(fieldPath, ".")

	if fieldPath == "" {
		return "", "", "", fmt.Errorf("empty field path in gate expression %q", raw)
	}
	if value == "" {
		return "", "", "", fmt.Errorf("empty value in gate expression %q", raw)
	}

	return fieldPath, operator, value, nil
}
