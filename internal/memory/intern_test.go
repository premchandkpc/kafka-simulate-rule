package memory

import (
	"sync"
	"testing"
)

func TestInternTable(t *testing.T) {
	tbl := NewInternTable([]string{"content-type", "accept"})

	if tbl.Len() != 2 {
		t.Errorf("expected 2 interned keys, got %d", tbl.Len())
	}

	id1 := tbl.Intern("content-type")
	id2 := tbl.Intern("content-type")
	if id1 != id2 {
		t.Errorf("same string should return same ID: %d vs %d", id1, id2)
	}

	id3 := tbl.Intern("x-custom")
	if id3 <= id2 {
		t.Errorf("new key should get a new ID, got %d <= %d", id3, id2)
	}

	if s := tbl.Lookup(id1); s != "content-type" {
		t.Errorf("Lookup(%d) = %q, want %q", id1, s, "content-type")
	}

	if s := tbl.Lookup(999); s != "" {
		t.Errorf("Lookup(999) = %q, want empty", s)
	}
}

func TestInternTableConcurrent(t *testing.T) {
	tbl := NewInternTable(nil)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_ = tbl.Intern("shared-key")
			_ = tbl.Intern("key-from-goroutine")
		}(i)
	}
	wg.Wait()

	// Both keys should be interned exactly once
	id1 := tbl.Intern("shared-key")
	id2 := tbl.Intern("shared-key")
	if id1 != id2 {
		t.Errorf("concurrent intern should return consistent IDs: %d vs %d", id1, id2)
	}
}

func TestInternLookupAfterIntern(t *testing.T) {
	tbl := NewInternTable(nil)
	id := tbl.Intern("test-key")
	if s := tbl.Lookup(id); s != "test-key" {
		t.Errorf("Lookup(%d) = %q, want %q", id, s, "test-key")
	}
}
