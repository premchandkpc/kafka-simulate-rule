package reliability

import (
	"sync"
	"sync/atomic"
	"time"
)

// BreakerState tracks the state of a circuit breaker.
type BreakerState int32

const (
	BreakerClosed   BreakerState = 0
	BreakerOpen     BreakerState = 1
	BreakerHalfOpen BreakerState = 2
)

// CircuitBreaker implements per-target circuit breaking.
type CircuitBreaker struct {
	target      string
	state       atomic.Int32
	failures    atomic.Int64
	lastFailure atomic.Int64
	threshold   int64
	resetAfter  time.Duration
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(target string, threshold int64, resetAfter time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		target:     target,
		threshold:  threshold,
		resetAfter: resetAfter,
	}
}

// Allow returns true if the request should be allowed through.
func (cb *CircuitBreaker) Allow() bool {
	switch BreakerState(cb.state.Load()) {
	case BreakerClosed:
		return true
	case BreakerOpen:
		last := time.Unix(0, cb.lastFailure.Load())
		if time.Since(last) > cb.resetAfter {
			cb.state.CompareAndSwap(int32(BreakerOpen), int32(BreakerHalfOpen))
			return true
		}
		return false
	case BreakerHalfOpen:
		return true
	}
	return false
}

// RecordSuccess records a successful call, resetting the breaker.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.failures.Store(0)
	cb.state.Store(int32(BreakerClosed))
}

// RecordFailure records a failed call, potentially opening the breaker.
func (cb *CircuitBreaker) RecordFailure() {
	n := cb.failures.Add(1)
	cb.lastFailure.Store(time.Now().UnixNano())
	if n >= cb.threshold {
		cb.state.Store(int32(BreakerOpen))
	}
}

// State returns the current breaker state.
func (cb *CircuitBreaker) State() BreakerState {
	return BreakerState(cb.state.Load())
}

// BreakerRegistry manages circuit breakers for multiple targets.
type BreakerRegistry struct {
	mu        sync.RWMutex
	breakers  map[string]*CircuitBreaker
	threshold int64
	resetDur  time.Duration
}

// NewBreakerRegistry creates a new breaker registry.
func NewBreakerRegistry(threshold int64, resetAfter time.Duration) *BreakerRegistry {
	return &BreakerRegistry{
		breakers:  make(map[string]*CircuitBreaker),
		threshold: threshold,
		resetDur:  resetAfter,
	}
}

// Register creates a circuit breaker for the given target.
func (r *BreakerRegistry) Register(target string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.breakers[target]; !ok {
		r.breakers[target] = NewCircuitBreaker(target, r.threshold, r.resetDur)
	}
}

// Get returns the circuit breaker for the given target.
func (r *BreakerRegistry) Get(target string) *CircuitBreaker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.breakers[target]
}

// Allow checks if a target is available.
func (r *BreakerRegistry) Allow(target string) bool {
	cb := r.Get(target)
	if cb == nil {
		return true
	}
	return cb.Allow()
}

// RecordSuccess records a success for the target.
func (r *BreakerRegistry) RecordSuccess(target string) {
	cb := r.Get(target)
	if cb != nil {
		cb.RecordSuccess()
	}
}

// RecordFailure records a failure for the target.
func (r *BreakerRegistry) RecordFailure(target string) {
	cb := r.Get(target)
	if cb != nil {
		cb.RecordFailure()
	}
}
