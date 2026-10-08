package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/flowrule/flowrule/internal/adapters/sql"
	"github.com/flowrule/flowrule/internal/codegen"
	"github.com/flowrule/flowrule/internal/domain"
)

type ContractHandler struct {
	db *sql.DB
}

func NewContractHandler(db *sql.DB) *ContractHandler {
	return &ContractHandler{db: db}
}

func (h *ContractHandler) CreateContract(w http.ResponseWriter, r *http.Request) {
	var schema domain.ContractSchema
	if err := json.NewDecoder(r.Body).Decode(&schema); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
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

	registry := sql.NewContractRegistry(h.db.Pool())
	if err := registry.Register(r.Context(), &schema); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(schema)
}

func (h *ContractHandler) ListContracts(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")

	registry := sql.NewContractRegistry(h.db.Pool())
	schemas, err := registry.List(r.Context(), name)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(schemas)
}

func (h *ContractHandler) GetContract(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	version := r.URL.Query().Get("version")

	if version == "" {
		http.Error(w, `{"error":"version query parameter is required"}`, http.StatusBadRequest)
		return
	}

	registry := sql.NewContractRegistry(h.db.Pool())
	schema, err := registry.Get(r.Context(), name, version)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if schema == nil {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(schema)
}

func (h *ContractHandler) GetContractVersion(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	version := r.PathValue("version")

	registry := sql.NewContractRegistry(h.db.Pool())
	schema, err := registry.Get(r.Context(), name, version)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if schema == nil {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(schema)
}

func (h *ContractHandler) DeleteContract(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	version := r.PathValue("version")

	registry := sql.NewContractRegistry(h.db.Pool())
	if err := registry.Delete(r.Context(), name, version); err != nil {
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	if req.Language == "" {
		http.Error(w, `{"error":"language is required (go, java, protobuf, jsonschema)"}`, http.StatusBadRequest)
		return
	}

	registry := sql.NewContractRegistry(h.db.Pool())
	schema, err := registry.Get(r.Context(), name, version)
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
	json.NewEncoder(w).Encode(map[string]interface{}{
		"contract":       name,
		"version":        version,
		"language":       req.Language,
		"output_dir":     outputDir,
		"generated_files": generatedFiles,
	})
}