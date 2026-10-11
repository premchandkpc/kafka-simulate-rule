package codegen

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSanitizeGeneratedOutputDirAllowsSafePath(t *testing.T) {
	base := t.TempDir()
	want := filepath.Join(base, "orders", "v1")

	got, err := sanitizeGeneratedOutputDir(want)
	if err != nil {
		t.Fatalf("sanitizeGeneratedOutputDir() error = %v", err)
	}
	if got != filepath.Clean(want) {
		t.Fatalf("sanitizeGeneratedOutputDir() = %q, want %q", got, filepath.Clean(want))
	}
}

func TestSanitizeGeneratedOutputDirRejectsParentTraversal(t *testing.T) {
	for _, candidate := range []string{
		filepath.Join("..", "outside"),
		filepath.Join("..", "..", "etc"),
		filepath.Join("orders", "..", "..", "outside"),
	} {
		if _, err := sanitizeGeneratedOutputDir(candidate); err == nil {
			t.Fatalf("sanitizeGeneratedOutputDir(%q) accepted parent traversal", candidate)
		}
	}
}

func TestSanitizeGeneratedOutputDirRejectsEmptyPath(t *testing.T) {
	if _, err := sanitizeGeneratedOutputDir(""); err == nil {
		t.Fatal("sanitizeGeneratedOutputDir(\"\") accepted empty path")
	}
}
