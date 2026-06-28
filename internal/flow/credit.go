package flow

import (
	"sync"
	"sync/atomic"
)

// CreditController implements per-service backpressure via token credits.
type CreditController struct {
	credits sync.Map
	initial int32
}

// NewCreditController creates a new credit controller.
func NewCreditController(initial int32) *CreditController {
	return &CreditController{
		initial: initial,
	}
}

// Register creates a credit bucket for the given target.
func (c *CreditController) Register(target string) {
	v := &atomic.Int32{}
	v.Store(c.initial)
	c.credits.Store(target, v)
}

// CanSend returns true if the target has credits available.
func (c *CreditController) CanSend(target string) bool {
	v, ok := c.credits.Load(target)
	if !ok {
		return true
	}
	return v.(*atomic.Int32).Load() > 0
}

// Debit reduces credits for the target.
func (c *CreditController) Debit(target string) {
	if v, ok := c.credits.Load(target); ok {
		v.(*atomic.Int32).Add(-1)
	}
}

// Credit restores a credit for the target.
func (c *CreditController) Credit(target string) {
	if v, ok := c.credits.Load(target); ok {
		v.(*atomic.Int32).Add(1)
	}
}
