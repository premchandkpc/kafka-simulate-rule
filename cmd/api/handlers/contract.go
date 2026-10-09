package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/flowrule/flowrule/internal/codegen"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type ContractHandler struct {
	registry ports.ContractRegistry
}

func NewContractHandler(registry ports.ContractRegistry) *ContractHandler {
	return &ContractHandler{registry: registry}
}

func (h *ContractHandler) CreateContract(w http.ResponseWriter, r *http.Request) {
	var schema domain.ContractSchema
	if err := decodeRequestBody(r, &schema); err != nil {
		http.Error(w, `{"error":"invalid request body: `+err.Error()+`"}`, http.StatusBadRequest)
		return
	}

	if schema.Name == "" {
		http.Error(w, `{"error":"name is required"}`, http.StatusBadRequest)
		return
	}
	if schema.Version == "" {
		http.Error(w, `{"error":"version is required"}`, http.StatusBadRequest)
		return
	}

	if err := h.registry.Register(r.Context(), &schema); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(schema); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *ContractHandler) ListContracts(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")

	schemas, err := h.registry.List(r.Context(), name)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(schemas); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *ContractHandler) GetContract(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	version := r.URL.Query().Get("version")

	if version == "" {
		http.Error(w, `{"error":"version query parameter is required"}`, http.StatusBadRequest)
		return
	}

	schema, err := h.registry.Get(r.Context(), name, version)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if schema == nil {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(schema); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *ContractHandler) GetContractVersion(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	version := r.PathValue("version")

	schema, err := h.registry.Get(r.Context(), name, version)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if schema == nil {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(schema); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *ContractHandler) DeleteContract(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	version := r.PathValue("version")

	if err := h.registry.Delete(r.Context(), name, version); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *ContractHandler) GenerateContract(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	version := r.PathValue("version")

	var req struct {
		Language string `json:"language"`
		Output   string `json:"output_dir,omitempty"`
	}
	if err := decodeRequestBody(r, &req); err != nil {
		http.Error(w, `{"error":"invalid request body: `+err.Error()+`"}`, http.StatusBadRequest)
		return
	}

	if req.Language == "" {
		http.Error(w, `{"error":"language is required (go, java, protobuf, jsonschema)"}`, http.StatusBadRequest)
		return
	}

	schema, err := h.registry.Get(r.Context(), name, version)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if schema == nil {
		http.Error(w, `{"error":"contract not found"}`, http.StatusNotFound)
		return
	}

	outputDir := req.Output
	if outputDir == "" {
		outputDir = filepath.Join(os.TempDir(), "flowrule-generated", name, version)
	}

	generator := codegen.NewGenerator(schema)
	var generatedFiles []string
	var genErr error

	switch strings.ToLower(req.Language) {
	case "go":
		generatedFiles, genErr = generator.GenerateGo(outputDir)
	case "java":
		generatedFiles, genErr = generator.GenerateJava(outputDir)
	case "protobuf":
		generatedFiles, genErr = generator.GenerateProtobuf(outputDir)
	case "jsonschema":
		generatedFiles, genErr = generator.GenerateJSONSchema(outputDir)
	default:
		http.Error(w, `{"error":"unsupported language"}`, http.StatusBadRequest)
		return
	}

	if genErr != nil {
		log.Printf("Code generation failed: %v", genErr)
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, genErr.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"contract":        name,
		"version":         version,
		"language":        req.Language,
		"output_dir":      outputDir,
		"generated_files": generatedFiles,
	}); err != nil {
		log.Printf("encode response: %v", err)
	}
}

// decodeRequestBody decodes the request body as JSON or YAML based on Content-Type header
func decodeRequestBody(r *http.Request, v interface{}) error {
	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "yaml") || strings.Contains(contentType, "yml") {
		return yaml.NewDecoder(r.Body).Decode(v)
	}
	return json.NewDecoder(r.Body).Decode(v)
}
