package observability

import (
	"log"
	"sync/atomic"
)

var (
	messagesProcessed atomic.Int64
	activeRules       atomic.Int64
)

func RecordMessage(ruleID, status string) {
	messagesProcessed.Add(1)
	log.Printf("msg processed: rule=%s status=%s total=%d", ruleID, status, messagesProcessed.Load())
}

func RecordDuration(ruleID string, seconds float64) {
	log.Printf("duration: rule=%s seconds=%f", ruleID, seconds)
}

func SetActiveRules(n int64) {
	activeRules.Store(n)
}
