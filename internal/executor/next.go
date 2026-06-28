package executor

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/premchand/flowrule/internal/dsl"
)

// HTTPCaller sends HTTP requests to downstream services.
type HTTPCaller interface {
	Call(ctx context.Context, target string, body []byte) ([]byte, error)
}

// HTTPCallerFunc is a function adapter that implements HTTPCaller.
type HTTPCallerFunc func(ctx context.Context, target string, body []byte) ([]byte, error)

func (f HTTPCallerFunc) Call(ctx context.Context, target string, body []byte) ([]byte, error) {
	return f(ctx, target, body)
}

// CircuitBreakerClient checks whether a target is available.
type CircuitBreakerClient interface {
	Allow(target string) bool
	RecordSuccess(target string)
	RecordFailure(target string)
}

// CreditClient manages per-target backpressure.
type CreditClient interface {
	CanSend(target string) bool
	Debit(target string)
	Credit(target string)
}

// execNext sends msg to instr.Targets[0] and waits for response.
// Retry is embedded when instr.RetryN > 0.
func (e *Executor) execNext(ctx context.Context, msg *Message, instr dsl.Instruction) {
	target := instr.Targets[0]

	if !e.credits.CanSend(target) {
		msg.failed = true
		msg.Errors = append(msg.Errors, StageError{
			Stage: "next", Target: target,
			Error: "credit exhausted", Timestamp: time.Now(),
		})
		return
	}

	if !e.breakers.Allow(target) {
		msg.failed = true
		msg.Errors = append(msg.Errors, StageError{
			Stage: "next", Target: target,
			Error: "circuit open", Timestamp: time.Now(),
		})
		return
	}

	callCtx := ctx
	if instr.TimeoutMs > 0 {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, time.Duration(instr.TimeoutMs)*time.Millisecond)
		defer cancel()
	}

	var lastErr error
	attempts := 1 + instr.RetryN
	backoff := 100 * time.Millisecond

	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			jitter := time.Duration(rand.Int63n(int64(backoff) / 2))
			sleep := backoff + jitter
			select {
			case <-callCtx.Done():
				msg.failed = true
				msg.Errors = append(msg.Errors, StageError{
					Stage: "next", Target: target,
					Error:   fmt.Sprintf("timeout during retry %d: %s", attempt, callCtx.Err()),
					Retries: attempt, Timestamp: time.Now(),
				})
				e.breakers.RecordFailure(target)
				return
			case <-time.After(sleep):
			}
			backoff *= 2
			if backoff > 10*time.Second {
				backoff = 10 * time.Second
			}
		}

		e.credits.Debit(target)
		resp, err := e.caller.Call(callCtx, target, msg.LastResponse)
		if err == nil {
			e.credits.Credit(target)
			e.breakers.RecordSuccess(target)
			msg.LastResponse = resp
			msg.HopCount++
			msg.failed = false
			return
		}
		e.credits.Credit(target)
		lastErr = err
		e.breakers.RecordFailure(target)
	}

	msg.failed = true
	msg.Errors = append(msg.Errors, StageError{
		Stage: "next", Target: target,
		Error:   lastErr.Error(),
		Retries: instr.RetryN, Timestamp: time.Now(),
	})
}
