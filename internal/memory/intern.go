package memory

import "sync"

// InternTable interns strings to uint16 IDs for efficient header key storage.
type InternTable struct {
	mu   sync.RWMutex
	fwd  map[string]uint16
	rev  []string
	next uint16
}

// NewInternTable creates a new InternTable, optionally pre-populated with well-known keys.
func NewInternTable(wellKnown []string) *InternTable {
	t := &InternTable{
		fwd: make(map[string]uint16),
		rev: make([]string, 0, len(wellKnown)),
	}
	for _, k := range wellKnown {
		t.Intern(k)
	}
	return t
}

// Intern returns the uint16 ID for s, registering it if new.
func (t *InternTable) Intern(s string) uint16 {
	t.mu.RLock()
	if id, ok := t.fwd[s]; ok {
		t.mu.RUnlock()
		return id
	}
	t.mu.RUnlock()

	t.mu.Lock()
	defer t.mu.Unlock()

	if id, ok := t.fwd[s]; ok {
		return id
	}

	id := t.next
	t.next++
	t.fwd[s] = id
	t.rev = append(t.rev, s)
	return id
}

// Lookup returns the string for a given uint16 ID.
// Safe without lock: rev is append-only, indices never change.
func (t *InternTable) Lookup(id uint16) string {
	if int(id) < len(t.rev) {
		return t.rev[id]
	}
	return ""
}

// Len returns the number of interned strings.
func (t *InternTable) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.rev)
}
