package vm

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/premchand/flowrule/internal/bytecode"
	"github.com/rs/zerolog"
)

// Message is the runtime value flowing through the VM.
type Message struct {
	ID            uint64
	CorrelationID string
	TraceID       string
	RuleID        string
	PlanVersion   int64
	PartitionKey  string
	Body          []byte
	ContentType   string
	Headers       map[string]string
	ReceivedAt    time.Time
	Deadline      time.Time
	HopCount      int
	Stage         string
	LastResponse  []byte
	failed        bool
	Errors        []StageError
}

type StageError struct {
	Stage     string
	Target    string
	Error     string
	Timestamp time.Time
	Retries   int
}

func (m *Message) Failed() bool     { return m.failed }
func (m *Message) SetFailed(v bool) { m.failed = v }

// Caller sends requests to downstream services.
type Caller interface {
	Call(ctx context.Context, target string, body []byte) ([]byte, error)
}

// Emitter sends fire-and-forget messages.
type Emitter interface {
	Emit(ctx context.Context, target string, body []byte) error
}

// Breaker controls circuit breaking.
type Breaker interface {
	Allow(target string) bool
	RecordSuccess(target string)
	RecordFailure(target string)
}

// Credit controls backpressure.
type Credit interface {
	CanSend(target string) bool
	Debit(target string)
	Credit(target string)
}

// VM executes compiled FlowRule bytecode modules.
type VM struct {
	caller   Caller
	emitter  Emitter
	breaker  Breaker
	credit   Credit
	log      zerolog.Logger
}

func New(caller Caller, emitter Emitter, breaker Breaker, credit Credit, log zerolog.Logger) *VM {
	return &VM{
		caller:  caller,
		emitter: emitter,
		breaker: breaker,
		credit:  credit,
		log:     log,
	}
}

// Execute runs a bytecode module against a message.
func (vm *VM) Execute(ctx context.Context, msg *Message, mod *bytecode.Module) error {
	cp := mod.ConstPool
	tls := mod.TargetLists
	instrs := mod.Instrs
	ip := uint32(0)

	for ip < uint32(len(instrs)) {
		instr := instrs[ip]
		msg.Stage = instr.Opcode.String()

		start := time.Now()

		switch instr.Opcode {
		case bytecode.OpNop:
			// no-op

		case bytecode.OpNext:
			target := resolveURL(cp, &instr)
			if target == "" {
				return fmt.Errorf("vm: empty target at ip=%d", ip)
			}

			if !vm.credit.CanSend(target) {
				msg.failed = true
				msg.Errors = append(msg.Errors, StageError{
					Stage: "next", Target: target, Error: "credit exhausted",
				})
				if !fallbackAfter(instrs, ip) {
					return fmt.Errorf("vm: n:%s failed with no fallback", target)
				}
				ip++
				continue
			}
			if !vm.breaker.Allow(target) {
				msg.failed = true
				msg.Errors = append(msg.Errors, StageError{
					Stage: "next", Target: target, Error: "circuit open",
				})
				if !fallbackAfter(instrs, ip) {
					return fmt.Errorf("vm: n:%s circuit open with no fallback", target)
				}
				ip++
				continue
			}

			callCtx := ctx
			timeoutMs := timeoutFromInstr(&instr)
			if timeoutMs > 0 {
				var cancel context.CancelFunc
				callCtx, cancel = context.WithTimeout(ctx, time.Duration(timeoutMs)*time.Millisecond)
				defer cancel()
			}

			retryN := retryFromInstr(&instr)
			attempts := 1 + retryN
			backoff := 100 * time.Millisecond
			var lastErr error

			for attempt := 0; attempt < attempts; attempt++ {
				if attempt > 0 {
					jitter := time.Duration(rand.Int63n(int64(backoff) / 2))
					select {
					case <-callCtx.Done():
						msg.failed = true
						msg.Errors = append(msg.Errors, StageError{
							Stage: "next", Target: target,
							Error: fmt.Sprintf("timeout on retry %d", attempt),
						})
						vm.breaker.RecordFailure(target)
						return fmt.Errorf("vm: n:%s timeout on retry %d", target, attempt)
					case <-time.After(backoff + jitter):
					}
					backoff *= 2
					if backoff > 10*time.Second {
						backoff = 10 * time.Second
					}
				}

				vm.credit.Debit(target)
				resp, err := vm.caller.Call(callCtx, target, msg.LastResponse)
				if err == nil {
					vm.credit.Credit(target)
					vm.breaker.RecordSuccess(target)
					msg.LastResponse = resp
					msg.HopCount++
					msg.failed = false
					goto nextDone
				}
				vm.credit.Credit(target)
				lastErr = err
				vm.breaker.RecordFailure(target)
			}

			msg.failed = true
			msg.Errors = append(msg.Errors, StageError{
				Stage: "next", Target: target,
				Error: lastErr.Error(), Retries: retryN, Timestamp: time.Now(),
			})
			if !fallbackAfter(instrs, ip) {
				return fmt.Errorf("vm: n:%s failed: %s", target, lastErr)
			}

		nextDone:
			vm.log.Debug().
				Str("op", "next").Str("target", target).
				Bool("failed", msg.failed).Dur("dur", time.Since(start)).
				Msg("hop")

		case bytecode.OpParallel:
			listIdx := instr.Arg1
			if uint32(len(tls)) <= listIdx {
				return fmt.Errorf("vm: invalid target list index %d", listIdx)
			}
			tl := tls[listIdx]
			results := make([][]byte, len(tl.Indices))
			errCh := make(chan error, len(tl.Indices))

			for i, cpIdx := range tl.Indices {
				i, cpIdx := i, cpIdx
				go func() {
					if uint32(len(cp)) <= cpIdx {
						errCh <- fmt.Errorf("vm: const pool index %d out of range", cpIdx)
						return
					}
					target := string(cp[cpIdx].Payload)
					resp, err := vm.caller.Call(ctx, target, msg.LastResponse)
					if err != nil {
						vm.breaker.RecordFailure(target)
						errCh <- fmt.Errorf("parallel %s: %w", target, err)
						return
					}
					vm.breaker.RecordSuccess(target)
					results[i] = resp
					errCh <- nil
				}()
			}

			var hasErr bool
			for range tl.Indices {
				if err := <-errCh; err != nil {
					hasErr = true
					msg.Errors = append(msg.Errors, StageError{
						Stage: "parallel", Error: err.Error(), Timestamp: time.Now(),
					})
				}
			}
			if hasErr {
				msg.failed = true
			}

		case bytecode.OpCollect:
			if msg.failed {
				// parallel failed; collect is a no-op on failure
				if !fallbackAfter(instrs, ip) {
					return fmt.Errorf("vm: parallel+collect failed with no fallback")
				}
				ip++
				continue
			}
			msg.HopCount++
			vm.log.Debug().
				Str("op", "collect").Dur("dur", time.Since(start)).Msg("collect")

		case bytecode.OpFallback:
			if msg.failed {
				target := string(cp[instr.Arg1].Payload)
				msg.failed = false
				msg.Errors = nil
				resp, err := vm.caller.Call(ctx, target, msg.LastResponse)
				if err != nil {
					msg.failed = true
					return fmt.Errorf("vm: fallback %s: %w", target, err)
				}
				msg.LastResponse = resp
				msg.HopCount++
			}

		case bytecode.OpGate:
			field := string(cp[instr.Arg1].Payload)
			operator := string(cp[instr.Arg2].Payload)
			value := string(cp[instr.Arg3].Payload)
			pass, err := evalGate(msg, field, operator, value)
			if err != nil {
				return fmt.Errorf("vm: gate eval at ip=%d: %w", ip, err)
			}
			if !pass {
				ip = skipToPipe(instrs, ip)
			}

		case bytecode.OpPipe:
			ip = skipToEnd(instrs, ip)

		case bytecode.OpEmit:
			listIdx := instr.Arg1
			if uint32(len(tls)) <= listIdx {
				return fmt.Errorf("vm: invalid emit list index %d", listIdx)
			}
			for _, cpIdx := range tls[listIdx].Indices {
				target := string(cp[cpIdx].Payload)
				go func(t string) {
					emitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					vm.emitter.Emit(emitCtx, t, msg.LastResponse)
				}(target)
			}

		case bytecode.OpDrop:
			vm.log.Debug().Str("op", "drop").Msg("message dropped")
			return nil

		case bytecode.OpMap:
			if uint32(len(mod.MapExprs)) <= instr.Arg1 {
				return fmt.Errorf("vm: invalid map expr index %d", instr.Arg1)
			}
			body := msg.LastResponse
			if len(body) == 0 {
				body = msg.Body
			}
			result, err := evalMapExpr(mod.MapExprs[instr.Arg1], cp, body)
			if err != nil {
				return fmt.Errorf("vm: map at ip=%d: %w", ip, err)
			}
			msg.LastResponse = result

		case bytecode.OpKey, bytecode.OpSplit:
			if uint32(len(cp)) <= instr.Arg1 {
				return fmt.Errorf("vm: const pool index %d out of range", instr.Arg1)
			}
			msg.PartitionKey = extractField(msg.LastResponse, string(cp[instr.Arg1].Payload))

		case bytecode.OpBuffer:
			return fmt.Errorf("vm: OpBuffer must be handled at engine level")

		case bytecode.OpJump:
			if instr.Arg1 >= uint32(len(instrs)) {
				return fmt.Errorf("vm: jump target %d out of bounds", instr.Arg1)
			}
			ip = instr.Arg1
			continue

		case bytecode.OpJumpIf:
			if msg.failed {
				if instr.Arg1 >= uint32(len(instrs)) {
					return fmt.Errorf("vm: jump_if target %d out of bounds", instr.Arg1)
				}
				ip = instr.Arg1
				continue
			}

		case bytecode.OpJumpIfN:
			if !msg.failed {
				if instr.Arg1 >= uint32(len(instrs)) {
					return fmt.Errorf("vm: jump_ifn target %d out of bounds", instr.Arg1)
				}
				ip = instr.Arg1
				continue
			}

		default:
			return fmt.Errorf("vm: unknown opcode %02x at ip=%d", instr.Opcode, ip)
		}

		ip++
	}

	return nil
}

// resolveURL extracts the target URL from a NEXT/FALLBACK instruction.
func resolveURL(cp []bytecode.ConstEntry, instr *bytecode.Instruction) string {
	var idx uint32
	if instr.Flags&bytecode.FlagHasTimeout != 0 && instr.Flags&bytecode.FlagHasRetry != 0 {
		// arg1=timeout, arg2=retry, arg3=url
		idx = instr.Arg3
	} else if instr.Flags&bytecode.FlagHasTimeout != 0 {
		// arg1=timeout, arg2=url
		idx = instr.Arg2
	} else if instr.Flags&bytecode.FlagHasRetry != 0 {
		// arg1=url, arg2=retry
		idx = instr.Arg1
	} else {
		idx = instr.Arg1
	}
	if uint32(len(cp)) <= idx {
		return ""
	}
	return string(cp[idx].Payload)
}

func timeoutFromInstr(instr *bytecode.Instruction) uint32 {
	if instr.Flags&bytecode.FlagHasTimeout != 0 {
		return instr.Arg1
	}
	return 0
}

func retryFromInstr(instr *bytecode.Instruction) int {
	if instr.Flags&bytecode.FlagHasRetry != 0 {
		if instr.Flags&bytecode.FlagHasTimeout != 0 {
			return int(instr.Arg2)
		}
		return int(instr.Arg2)
	}
	return 0
}

func fallbackAfter(instrs []bytecode.Instruction, i uint32) bool {
	for j := i + 1; j < uint32(len(instrs)); j++ {
		switch instrs[j].Opcode {
		case bytecode.OpPipe:
			return false
		case bytecode.OpFallback, bytecode.OpJumpIf:
			return true
		}
	}
	return false
}

func skipToPipe(instrs []bytecode.Instruction, i uint32) uint32 {
	for j := i + 1; j < uint32(len(instrs)); j++ {
		if instrs[j].Opcode == bytecode.OpPipe {
			return j
		}
	}
	return uint32(len(instrs)) - 1
}

func skipToEnd(instrs []bytecode.Instruction, i uint32) uint32 {
	for j := i + 1; j < uint32(len(instrs)); j++ {
		if instrs[j].Opcode == bytecode.OpPipe {
			return j
		}
	}
	return uint32(len(instrs)) - 1
}

func evalGate(msg *Message, field, operator, value string) (bool, error) {
	body := msg.LastResponse
	if len(body) == 0 {
		body = msg.Body
	}
	val, err := extractJSONField(body, strings.Split(field, "."))
	if err != nil {
		return false, err
	}
	return compareValues(val, operator, value)
}

func extractJSONField(body []byte, path []string) (string, error) {
	var data any
	if err := json.Unmarshal(body, &data); err != nil {
		return "", fmt.Errorf("unmarshal: %w", err)
	}
	current := data
	for _, part := range path {
		m, ok := current.(map[string]any)
		if !ok {
			return "", fmt.Errorf("field %q not found", part)
		}
		v, ok := m[part]
		if !ok {
			return "", fmt.Errorf("field %q not found", part)
		}
		current = v
	}
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
	default:
		b, _ := json.Marshal(v)
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
			return false, fmt.Errorf("numeric compare: %q vs %q", val, target)
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
	return false, fmt.Errorf("unknown operator %q", operator)
}

func extractField(body []byte, field string) string {
	if len(body) == 0 || field == "" {
		return ""
	}
	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		return ""
	}
	v, ok := data[field]
	if !ok {
		return ""
	}
	return fmt.Sprint(v)
}

func evalMapExpr(me bytecode.MapExprEntry, cp []bytecode.ConstEntry, body []byte) ([]byte, error) {
	switch me.Type {
	case bytecode.MapExprFieldPath:
		// body encodes: segCount(2) + reserved(2) + segCPs(4*N)
		if len(me.Body) < 4 {
			return nil, fmt.Errorf("map expr: truncated field path")
		}
		segCount := uint16(me.Body[0]) | uint16(me.Body[1])<<8
		path := make([]string, segCount)
		for i := uint16(0); i < segCount; i++ {
			base := 4 + int(i)*4
			cpIdx := uint32(me.Body[base]) | uint32(me.Body[base+1])<<8 |
				uint32(me.Body[base+2])<<16 | uint32(me.Body[base+3])<<24
			if uint32(len(cp)) <= cpIdx {
				return nil, fmt.Errorf("map expr: cp index %d out of range", cpIdx)
			}
			path[i] = string(cp[cpIdx].Payload)
		}
		return extractJSONPath(body, path)

	default:
		return body, nil
	}
}

func extractJSONPath(body []byte, path []string) ([]byte, error) {
	var data any
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	current := data
	for _, part := range path {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("path %q not found", part)
		}
		v, ok := m[part]
		if !ok {
			return nil, fmt.Errorf("path %q not found", part)
		}
		current = v
	}
	return json.Marshal(current)
}
