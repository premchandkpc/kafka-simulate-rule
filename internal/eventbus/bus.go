package eventbus

import (
	"context"
	"sync"
	"time"
)

// EventType identifies a class of lifecycle events.
type EventType int

const (
	EventMsgStarted EventType = iota + 1
	EventMsgFinished
	EventMsgDropped
	EventMsgFailed
	EventHopSucceeded
	EventHopFailed
	EventRuleMatched
	EventPartitionAssigned
	EventEngineStarted
	EventEngineStopped
)

func (t EventType) String() string {
	switch t {
	case EventMsgStarted:
		return "msg.started"
	case EventMsgFinished:
		return "msg.finished"
	case EventMsgDropped:
		return "msg.dropped"
	case EventMsgFailed:
		return "msg.failed"
	case EventHopSucceeded:
		return "hop.succeeded"
	case EventHopFailed:
		return "hop.failed"
	case EventRuleMatched:
		return "rule.matched"
	case EventPartitionAssigned:
		return "partition.assigned"
	case EventEngineStarted:
		return "engine.started"
	case EventEngineStopped:
		return "engine.stopped"
	default:
		return "unknown"
	}
}

// Event is a typed lifecycle event.
type Event struct {
	Type  EventType
	MsgID uint64
	Data  map[string]string
	Time  time.Time
}

const busCap = 1000

// Bus is a typed pub/sub event bus.
type Bus struct {
	mu      sync.RWMutex
	subs    map[EventType][]chan Event
	started bool
	ctx     context.Context
	cancel  context.CancelFunc
}

func New() *Bus {
	return &Bus{
		subs: make(map[EventType][]chan Event),
	}
}

// Subscribe returns a channel that receives events of the given type.
func (b *Bus) Subscribe(typ EventType, cap int) <-chan Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan Event, cap)
	b.subs[typ] = append(b.subs[typ], ch)
	return ch
}

// Unsubscribe removes a channel from the bus.
func (b *Bus) Unsubscribe(typ EventType, ch <-chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	subs := b.subs[typ]
	for i, sub := range subs {
		if sub == ch {
			b.subs[typ] = append(subs[:i], subs[i+1:]...)
			return
		}
	}
}

// Publish sends an event to all subscribers of its type.
func (b *Bus) Publish(ev Event) {
	b.mu.RLock()
	subs := b.subs[ev.Type]
	b.mu.RUnlock()
	for _, ch := range subs {
		select {
		case ch <- ev:
		default:
			// drop if channel full
		}
	}
}

// Start starts the bus (no-op for now).
func (b *Bus) Start(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started {
		return
	}
	b.ctx, b.cancel = context.WithCancel(ctx)
	b.started = true
}

// Stop stops the bus and closes all subscriber channels.
func (b *Bus) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.started {
		return
	}
	b.cancel()
	for typ, subs := range b.subs {
		for _, ch := range subs {
			close(ch)
		}
		delete(b.subs, typ)
	}
	b.started = false
}
