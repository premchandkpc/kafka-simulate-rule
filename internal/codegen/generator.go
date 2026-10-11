package codegen

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/flowrule/flowrule/internal/domain"
)

// TargetGo identifies Go code generation.
const TargetGo = "go"

// TargetJava identifies Java code generation.
const TargetJava = "java"

// TargetProtobuf identifies Protocol Buffers code generation.
const TargetProtobuf = "protobuf"

// TargetJSONSchema identifies JSON Schema generation.
const TargetJSONSchema = "jsonschema"

// SupportedTargetsText is the user-facing list of supported generation targets.
const SupportedTargetsText = TargetGo + ", " + TargetJava + ", " + TargetProtobuf + ", " + TargetJSONSchema

const (
	goFileExtension         = ".go"
	javaFileExtension       = ".java"
	protobufFileExtension   = ".proto"
	jsonSchemaFileExtension = ".json"
	runtimeDirectoryName    = "runtime"
)

func runtimeOutputDir(outputDir string) string {
	return filepath.Join(outputDir, TargetGo, runtimeDirectoryName)
}

func sanitizeGeneratedOutputDir(outputDir string) (string, error) {
	if outputDir == "" {
		return "", fmt.Errorf("output directory is required")
	}

	cleaned := filepath.Clean(outputDir)
	if cleaned == "." {
		return cleaned, nil
	}
	if filepath.IsAbs(cleaned) || filepath.VolumeName(cleaned) != "" {
		return "", fmt.Errorf("output directory must be relative")
	}

	parts := strings.Split(cleaned, string(filepath.Separator))
	for _, part := range parts {
		if part == ".." {
			return "", fmt.Errorf("output directory must not contain parent traversal")
		}
	}

	return cleaned, nil
}

// ...existing file omitted for brevity...
