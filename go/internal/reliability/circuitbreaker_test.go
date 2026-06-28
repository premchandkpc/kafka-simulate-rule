package reliability

import (
	"testing"
	"time"
)

func TestInitiallyClosed(t *testing.T) {
	cb := NewCircuitBreaker(3, time.Second)
	if !cb.Allow() {
		t.Error("expected Allow() to return true initially")
	}
}

func TestTripsAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(3, time.Minute)
	for i := 0; i < 3; i++ {
		cb.Failure()
	}
	if cb.Allow() {
		t.Error("expected Allow() to return false after threshold failures")
	}
}

func TestSuccessResets(t *testing.T) {
	cb := NewCircuitBreaker(2, time.Minute)
	cb.Failure()
	cb.Failure()
	cb.Success()
	if !cb.Allow() {
		t.Error("expected Allow() to return true after Success()")
	}
}

func TestHalfOpenRecovery(t *testing.T) {
	cb := NewCircuitBreaker(1, 50*time.Millisecond)
	cb.Failure()

	if cb.Allow() {
		t.Error("expected rejected while open")
	}

	time.Sleep(60 * time.Millisecond)

	if !cb.Allow() {
		t.Error("expected allowed after recovery timeout (half-open)")
	}
}

func TestHalfOpenLimitsRequests(t *testing.T) {
	cb := NewCircuitBreaker(1, 50*time.Millisecond)
	cb.halfOpenMaxReqs = 2
	cb.Failure()

	time.Sleep(60 * time.Millisecond)

	// 1st call transitions open→half-open and is allowed
	if !cb.Allow() {
		t.Error("1st call (open→half-open) should be allowed")
	}
	// 2nd and 3rd calls are within the 2-request half-open limit
	if !cb.Allow() {
		t.Error("2nd half-open request should be allowed")
	}
	if !cb.Allow() {
		t.Error("3rd half-open request should be allowed")
	}
	// 4th call exceeds the limit
	if cb.Allow() {
		t.Error("4th half-open request should be rejected")
	}
}

func TestSuccessClosesFromHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(1, 50*time.Millisecond)
	cb.Failure()
	time.Sleep(60 * time.Millisecond)

	_ = cb.Allow()
	cb.Success()

	if !cb.Allow() {
		t.Error("expected closed after success in half-open")
	}
}
