package handlers

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSafeGenerationOutputDir(t *testing.T) {
	root := filepath.Join(t.TempDir(), "flowrule-generated")
	valid := filepath.Join(root, "orders", "v1")
	got, err := safeGenerationOutputDir(valid, root)
	if err != nil {
		t.Fatalf("safeGenerationOutputDir(valid) error = %v", err)
	}
	want, err := filepath.Abs(valid)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("output dir = %q, want %q", got, want)
	}
}

func TestSafeGenerationOutputDirRejectsSiblingPrefix(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "flowrule-generated")
	sibling := filepath.Join(parent, "flowrule-generated-evil")

	if _, err := safeGenerationOutputDir(sibling, root); err == nil {
		t.Fatal("expected sibling path to be rejected")
	}
}

func TestSafeGenerationOutputDirRejectsParentTraversal(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "flowrule-generated")
	escape := filepath.Join(root, "..", "outside")

	if _, err := safeGenerationOutputDir(escape, root); err == nil {
		t.Fatal("expected parent traversal to be rejected")
	}
}

func TestSafeGenerationOutputDirRejectsSymlinkEscape(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "flowrule-generated")
	outside := filepath.Join(parent, "outside")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	escape := filepath.Join(root, "link", "generated")
	if _, err := safeGenerationOutputDir(escape, root); err == nil {
		t.Fatal("expected symlink escape to be rejected")
	}
}
