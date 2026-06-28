package memory

import (
	"testing"
)

func TestArenaAlloc(t *testing.T) {
	a := GetArena(ArenaSmall)
	defer a.Release()

	buf := a.Alloc(100)
	if len(buf) != 100 {
		t.Fatalf("expected 100 bytes, got %d", len(buf))
	}
	// Write to verify we own the memory
	for i := range buf {
		buf[i] = byte(i)
	}
}

func TestArenaOverflow(t *testing.T) {
	a := GetArena(ArenaSmall)
	defer a.Release()

	// Allocate more than arena capacity
	buf := a.Alloc(ArenaSmall + 1)
	if len(buf) != ArenaSmall+1 {
		t.Fatalf("expected %d bytes, got %d", ArenaSmall+1, len(buf))
	}
}

func TestArenaOverflowCountIncremented(t *testing.T) {
	before := ArenaOverflowCount()
	a := GetArena(ArenaSmall)
	defer a.Release()

	a.Alloc(ArenaSmall + 1)
	after := ArenaOverflowCount()
	if after <= before {
		t.Error("expected overflow count to increase")
	}
}

func TestArenaReleaseToPool(t *testing.T) {
	a := GetArena(ArenaMedium)
	buf := a.Alloc(100)
	buf[0] = 42
	a.Release()

	// Get another arena from same pool
	b := GetArena(ArenaSmall)
	defer b.Release()
	_ = b
	// The arena should be reset (used=0)
	if a.used != 0 {
		t.Errorf("expected arena to be reset after release, used=%d", a.used)
	}
}

func TestSlabTierSelection(t *testing.T) {
	tests := []struct {
		size int
		cap  int
	}{
		{100, ArenaSmall},
		{ArenaSmall, ArenaSmall},
		{ArenaSmall + 1, ArenaMedium},
		{ArenaMedium, ArenaMedium},
		{ArenaMedium + 1, ArenaLarge},
		{ArenaLarge, ArenaLarge},
		{ArenaLarge + 1000, ArenaLarge},
	}

	for _, tt := range tests {
		a := GetArena(tt.size)
		if cap(a.buf) != tt.cap {
			t.Errorf("GetArena(%d): cap=%d, want %d", tt.size, cap(a.buf), tt.cap)
		}
		a.Release()
	}
}
