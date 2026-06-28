package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/premchand/flowrule/internal/dsl"
	"github.com/rs/zerolog"
)

// Logger is a subset of zerolog.Logger used by the executor.
type Logger interface {
	Info() *zerolog.Event
	Warn() *zerolog.Event
	Error() *zerolog.Event
	Debug() *zerolog.Event
}

// Executor executes compiled DSL pipelines for messages.
// One executor per worker goroutine — not shared between goroutines.
type Executor struct {
	caller   HTTPCaller
	breakers CircuitBreakerClient
	credits  CreditClient
	emitter  Emitter
	log      Logger
}

// New creates a new executor.
func New(caller HTTPCaller, breakers CircuitBreakerClient, credits CreditClient, emitter Emitter, log Logger) *Executor {
	return &Executor{
		caller:   caller,
		breakers: breakers,
		credits:  credits,
		emitter:  emitter,
		log:      log,
	}
}

// opName returns a human-readable name for an OpCode.
func opName(op dsl.OpCode) string {
	switch op {
	case dsl.OpNext:
		return "next"
	case dsl.OpParallel:
		return "parallel"
	case dsl.OpCollect:
		return "collect"
	case dsl.OpFallback:
		return "fallback"
	case dsl.OpGate:
		return "gate"
	case dsl.OpPipe:
		return "pipe"
	case dsl.OpEmit:
		return "emit"
	case dsl.OpDrop:
		return "drop"
	case dsl.OpMap:
		return "map"
	case dsl.OpKey:
		return "key"
	case dsl.OpSplit:
		return "split"
	case dsl.OpBuffer:
		return "buffer"
	default:
		return fmt.Sprintf("op(%d)", op)
	}
}

// lastErrorMsg returns the most recent error message.
func lastErrorMsg(msg *Message) string {
	if len(msg.Errors) == 0 {
		return "unknown error"
	}
	return msg.Errors[len(msg.Errors)-1].Error
}

// Execute runs a compiled pipeline for msg.
// Returns error only when pipeline is unrecoverable (engine will DLQ).
// msg.arena.Release() is NOT called here.
func (e *Executor) Execute(ctx context.Context, msg *Message, plan *dsl.ExecutionPlan) error {
	var parallelResults [][]byte

	for i := 0; i < len(plan.Instructions); i++ {
		instr := plan.Instructions[i]
		msg.Stage = opName(instr.Op)

		start := time.Now()

		switch instr.Op {
		case dsl.OpNext:
			e.execNext(ctx, msg, instr)
			e.log.Debug().
				Str("rule", plan.RuleID).
				Str("target", instr.Targets[0]).
				Str("op", "next").
				Dur("dur", time.Since(start)).
				Bool("failed", msg.failed).
				Msg("hop")
			if msg.failed {
				if !fallbackFollows(plan.Instructions, i) {
					return fmt.Errorf("executor: n:%s failed with no fallback: %s",
						instr.Targets[0], lastErrorMsg(msg))
				}
			}

		case dsl.OpParallel:
			parallelResults = e.execParallel(ctx, msg, instr)

		case dsl.OpCollect:
			if parallelResults == nil && !msg.failed {
				return fmt.Errorf("executor: c without preceding p: at index %d", i)
			}
			e.execCollect(msg, parallelResults)
			parallelResults = nil
			e.log.Debug().
				Str("rule", plan.RuleID).
				Str("op", "collect").
				Dur("dur", time.Since(start)).
				Bool("failed", msg.failed).
				Msg("collect")
			if msg.failed {
				if !fallbackFollows(plan.Instructions, i) {
					return fmt.Errorf("executor: parallel+collect failed with no fallback: %s",
						lastErrorMsg(msg))
				}
			}

		case dsl.OpFallback:
			if msg.failed {
				msg.failed = false
				msg.Errors = nil
				e.execNext(ctx, msg, instr)
				if msg.failed {
					return fmt.Errorf("executor: fallback %s also failed: %s",
						instr.Targets[0], lastErrorMsg(msg))
				}
			}

		case dsl.OpGate:
			pass, err := evalGate(msg, instr)
			if err != nil {
				return fmt.Errorf("executor: gate eval: %w", err)
			}
			if !pass {
				i = skipToPipe(plan.Instructions, i)
			}

		case dsl.OpPipe:
			i = skipToEnd(plan.Instructions, i)

		case dsl.OpEmit:
			e.execEmit(ctx, msg, instr)

		case dsl.OpDrop:
			return nil

		case dsl.OpMap:
			if err := e.execMap(msg, instr); err != nil {
				return fmt.Errorf("executor: map transform: %w", err)
			}

		case dsl.OpKey, dsl.OpSplit:
			msg.PartitionKey = extractFieldString(msg.LastResponse, instr.Operand)

		case dsl.OpBuffer:
			return fmt.Errorf("executor: OpBuffer must be handled at engine level")
		}
	}

	return nil
}

// extractFieldString extracts a top-level string field from JSON.
func extractFieldString(body []byte, field string) string {
	if len(body) == 0 || field == "" {
		return ""
	}
	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		return ""
	}
	val, ok := data[field]
	if !ok {
		return ""
	}
	switch v := val.(type) {
	case string:
		return v
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// fallbackFollows returns true if an OpFallback appears before the next OpPipe or end.
func fallbackFollows(instrs []dsl.Instruction, i int) bool {
	for j := i + 1; j < len(instrs); j++ {
		if instrs[j].Op == dsl.OpPipe {
			return false
		}
		if instrs[j].Op == dsl.OpFallback {
			return true
		}
	}
	return false
}

// withMessageContext injects message metadata into context.
func withMessageContext(ctx context.Context, msg *Message) context.Context {
	return ctx
}
