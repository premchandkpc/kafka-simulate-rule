// Package memory provides arena-based memory allocation for zero-GC message processing.
package memory

import "sync"

const (
	ArenaSmall  = 2048
	ArenaMedium = 8192
	ArenaLarge  = 65536
)

// arenaOverflowCount is a package-level counter for arena overflow events.
// Wired to prometheus in the observability package.
var arenaOverflowCount int64

// ArenaOverflowCount returns the number of times an arena fell back to heap.
func ArenaOverflowCount() int64 { return arenaOverflowCount }

// Arena is a bump allocator backed by a fixed byte slice.
// Not goroutine-safe — one arena per message, one message per goroutine.
type Arena struct {
	buf  []byte
	used int
	pool *sync.Pool
}

// Alloc returns a slice of n bytes from the arena.
// Falls back to heap if arena is full.
func (a *Arena) Alloc(n int) []byte {
	if n <= 0 {
		return nil
	}
	if a.used+n > len(a.buf) {
		arenaOverflowCount++
		return make([]byte, n)
	}
	s := a.buf[a.used : a.used+n : a.used+n]
	a.used += n
	return s
}

// Reset resets the arena pointer without freeing memory.
func (a *Arena) Reset() {
	a.used = 0
}

// Release resets the arena and returns it to its slab pool.
// MUST be called exactly once per message, on every exit path.
func (a *Arena) Release() {
	a.Reset()
	if a.pool != nil {
		a.pool.Put(a)
	}
}
