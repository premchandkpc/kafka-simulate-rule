package handlers

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/flowrule/flowrule/internal/adapters/http/responses"
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

func (h *ContractHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/contracts", h.CreateContract)
	mux.HandleFunc("GET /v1/contracts", h.ListContracts)
	mux.HandleFunc("GET /v1/contracts/{name}", h.GetContract)
	mux.HandleFunc("GET /v1/contracts/{name}/{version}", h.GetContractVersion)
	mux.HandleFunc("DELETE /v1/contracts/{name}/{version}", h.DeleteContract)
	mux.HandleFunc("POST /v1/contracts/{name}/{version}/generate", h.GenerateContract)
}

func (h *ContractHandler) CreateContract(w http.ResponseWriter, r *http.Request) {
	var schema domain.ContractSchema
	if err := decodeRequestBody(r, &schema); err != nil {
		responses.WriteError(w, http.StatusBadRequest, messageInvalidBodyPrefix+err.Error(), responses.CodeInvalidJSON)
		return
	}

	if schema.Name == "" {
		responses.WriteError(w, http.StatusBadRequest, "name is required", responses.CodeValidationFailed)
		return
	}
	if schema.Version == "" {
		responses.WriteError(w, http.StatusBadRequest, "version is required", responses.CodeValidationFailed)
		return
	}

	if err := h.registry.Register(r.Context(), &schema); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteCreated(w, schema)
}

func (h *ContractHandler) ListContracts(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")

	schemas, err := h.registry.List(r.Context(), name)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteOK(w, schemas)
}

func (h *ContractHandler) GetContract(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	version := r.URL.Query().Get(queryParamVersion)

	if version == "" {
		responses.WriteError(w, http.StatusBadRequest, "version query parameter is required", responses.CodeValidationFailed)
		return
	}

	schema, err := h.registry.Get(r.Context(), name, version)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if schema == nil {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}

	responses.WriteOK(w, schema)
}

func (h *ContractHandler) GetContractVersion(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	version := r.PathValue("version")

	schema, err := h.registry.Get(r.Context(), name, version)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if schema == nil {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}

	responses.WriteOK(w, schema)
}

func (h *ContractHandler) DeleteContract(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	version := r.PathValue("version")

	if err := h.registry.Delete(r.Context(), name, version); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteNoContent(w)
}

func (h *ContractHandler) GenerateContract(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	version := r.PathValue("version")

	var req struct {
		Language string `json:"language"`
		Output   string `json:"output_dir,omitempty"`
	}
	if err := decodeRequestBody(r, &req); err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), responses.CodeInvalidJSON)
		return
	}

	if req.Language == "" {
		responses.WriteError(w, http.StatusBadRequest, "language is required (go, java, protobuf, jsonschema)", responses.CodeValidationFailed)
		return
	}

	schema, err := h.registry.Get(r.Context(), name, version)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if schema == nil {
		responses.WriteError(w, http.StatusNotFound, "contract not found", responses.CodeNotFound)
		return
	}

	// Generated files must remain inside the dedicated temp root. Validate
	// path components as well as string containment so sibling-prefix paths and
	// symlinks cannot redirect writes outside the allowed directory.
	allowedBase := filepath.Join(os.TempDir(), "flowrule-generated")
	outputDir := req.Output
	if outputDir == "" {
		outputDir = filepath.Join(allowedBase, name, version)
	}
	var pathErr error
	outputDir, pathErr = safeGenerationOutputDir(outputDir, allowedBase)
	if pathErr != nil {
		responses.WriteError(w, http.StatusForbidden, "output directory not allowed", responses.CodeForbidden)
		return
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
		responses.WriteError(w, http.StatusBadRequest, "unsupported language", responses.CodeValidationFailed)
		return
	}

	if genErr != nil {
		responses.WriteError(w, http.StatusInternalServerError, genErr.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteOK(w, map[string]interface{}{
		"contract":        name,
		"version":         version,
		"language":        req.Language,
		"output_dir":      outputDir,
		"generated_files": generatedFiles,
	})
}
