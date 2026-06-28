package executor

import (
	"time"

	"github.com/premchand/flowrule/internal/memory"
)

type Message struct {
	ID            uint64
	CorrelationID string
	TraceID       string

	RuleID      string
	PlanVersion int64
	PartitionKey string

	Body        []byte
	ContentType string
	Headers     map[uint16]string

	ReceivedAt time.Time
	Deadline   time.Time
	HopCount   int
	Stage      string

	LastResponse []byte
	failed       bool
	Errors       []StageError

	arena *memory.Arena
}

type StageError struct {
	Stage     string
	Target    string
	Error     string
	Timestamp time.Time
	Retries   int
}

// Alloc allocates n bytes from the message's arena.
func (m *Message) Alloc(n int) []byte {
	if m.arena == nil {
		return make([]byte, n)
	}
	return m.arena.Alloc(n)
}

// Release returns the arena to its pool. Called exactly once per message.
func (m *Message) Release() {
	if m.arena != nil {
		m.arena.Release()
		m.arena = nil
	}
}

// Failed returns whether the current instruction has failed.
func (m *Message) Failed() bool { return m.failed }

// SetFailed marks the message as failed.
func (m *Message) SetFailed(v bool) { m.failed = v }
