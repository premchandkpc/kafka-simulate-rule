package handlers

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// safeGenerationOutputDir validates that a requested output path is contained
// by root. The directory is not created here; callers should use this only as
// an input validation layer, not as a substitute for race-safe file creation.
func safeGenerationOutputDir(outputDir, root string) (string, error) {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve output root: %w", err)
	}

	rootInfo, err := os.Lstat(rootAbs)
	if err != nil {
		return "", fmt.Errorf("inspect output root: %w", err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return "", fmt.Errorf("output root %q must be a real directory, not a symlink or file", rootAbs)
	}
	realRoot, err := filepath.EvalSymlinks(rootAbs)
	if err != nil {
		return "", fmt.Errorf("resolve output root: %w", err)
	}

	relativeOutput := outputDir
	if filepath.IsAbs(outputDir) {
		relativeOutput, err = filepath.Rel(rootAbs, outputDir)
		if err != nil {
			return "", fmt.Errorf("check output directory: %w", err)
		}
	}
	cleanOutput, err := sanitizeRelativeOutputPath(relativeOutput)
	if err != nil {
		return "", fmt.Errorf("resolve output directory: %w", err)
	}
	outputAbs := filepath.Join(rootAbs, cleanOutput)
	rel, err := filepath.Rel(rootAbs, outputAbs)
	if err != nil {
		return "", fmt.Errorf("check output directory: %w", err)
	}
	if !withinDirectory(rel) {
		return "", fmt.Errorf("output directory must be within %q", rootAbs)
	}

	// Resolve the nearest existing ancestor so symlinked path components
	// cannot redirect the eventual output outside the approved root.
	current := outputAbs
	var suffix []string
	for {
		_, statErr := os.Lstat(current)
		if statErr == nil {
			resolved, resolveErr := filepath.EvalSymlinks(current)
			if resolveErr != nil {
				return "", fmt.Errorf("resolve output path component %q: %w", current, resolveErr)
			}
			candidate := resolved
			for _, part := range suffix {
				// codeql[go/path-injection] Each segment is relative, traversal-rejected, and checked against the resolved root.
				candidate = filepath.Join(candidate, part)
			}
			resolvedRel, relErr := filepath.Rel(realRoot, candidate)
			if relErr != nil {
				return "", fmt.Errorf("check resolved output directory: %w", relErr)
			}
			if !withinDirectory(resolvedRel) {
				return "", fmt.Errorf("output directory resolves outside %q", realRoot)
			}
			break
		}
		if !os.IsNotExist(statErr) {
			return "", fmt.Errorf("inspect output path component %q: %w", current, statErr)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("no existing ancestor for output directory %q", outputAbs)
		}
		suffix = append([]string{filepath.Base(current)}, suffix...)
		current = parent
	}

	return outputAbs, nil
}

func sanitizeRelativeOutputPath(outputDir string) (string, error) {
	if outputDir == "" {
		return "", fmt.Errorf("output directory is required")
	}
	if filepath.IsAbs(outputDir) || filepath.VolumeName(outputDir) != "" {
		return "", fmt.Errorf("output directory must be a relative path")
	}

	cleaned := filepath.Clean(outputDir)
	if cleaned == "." {
		return ".", nil
	}

	parts := strings.Split(cleaned, string(filepath.Separator))
	for _, part := range parts {
		if part == ".." {
			return "", fmt.Errorf("output directory must not contain parent traversal")
		}
	}

	return cleaned, nil
}

func withinDirectory(rel string) bool {
	if filepath.IsAbs(rel) || rel == ".." {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
