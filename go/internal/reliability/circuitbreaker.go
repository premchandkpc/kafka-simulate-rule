package reliability

import (
	"sync"
	"sync/atomic"
	"time"
)

type State int32

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

type CircuitBreaker struct {
	mu                sync.Mutex
	state             State
	failureCount      int64
	lastFailureTime   time.Time
	threshold         int
	recoveryTimeout   time.Duration
	halfOpenMaxReqs   int
	halfOpenReqs      int64
}

func NewCircuitBreaker(threshold int, recoveryTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:           StateClosed,
		threshold:       threshold,
		recoveryTimeout: recoveryTimeout,
		halfOpenMaxReqs: 3,
	}
}

func (cb *CircuitBreaker) Allow() bool {
	state := State(atomic.LoadInt32((*int32)(&cb.state)))
	switch state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(cb.lastFailureTime) > cb.recoveryTimeout {
			atomic.StoreInt32((*int32)(&cb.state), int32(StateHalfOpen))
			atomic.StoreInt64(&cb.halfOpenReqs, 0)
			return true
		}
		return false
	case StateHalfOpen:
		n := atomic.AddInt64(&cb.halfOpenReqs, 1)
		return n <= int64(cb.halfOpenMaxReqs)
	}
	return false
}

func (cb *CircuitBreaker) Success() {
	atomic.StoreInt32((*int32)(&cb.state), int32(StateClosed))
	atomic.StoreInt64(&cb.failureCount, 0)
}

func (cb *CircuitBreaker) Failure() {
	atomic.AddInt64(&cb.failureCount, 1)
	cb.mu.Lock()
	cb.lastFailureTime = time.Now()
	cb.mu.Unlock()

	n := atomic.LoadInt64(&cb.failureCount)
	if n >= int64(cb.threshold) {
		atomic.StoreInt32((*int32)(&cb.state), int32(StateOpen))
	}
}
