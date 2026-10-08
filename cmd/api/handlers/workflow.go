package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/flowrule/flowrule/internal/adapters/sql"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	svcworkflow "github.com/flowrule/flowrule/internal/services/workflow"
	svcrules "github.com/flowrule/flowrule/internal/services/rules"
)

type WorkflowHandler struct {
	workflowSvc *svcworkflow.Service
	ruleSvc     *svcrules.Service
	pool        *sql.DB
}

func NewWorkflowHandler(workflowSvc *svcworkflow.Service, ruleSvc *svcrules.Service, pool *sql.DB) *WorkflowHandler {
	return &WorkflowHandler{
		workflowSvc: workflowSvc,
		ruleSvc:     ruleSvc,
		pool:        pool,
	}
}

func (h *WorkflowHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/workflows", h.CreateWorkflow)
	mux.HandleFunc("GET /v1/workflows/{workflowID}", h.GetWorkflow)
	mux.HandleFunc("POST /v1/workflows/{workflowID}/transition", h.TransitionWorkflow)
	mux.HandleFunc("GET /v1/workflows", h.ListWorkflows)
	mux.HandleFunc("GET /v1/workflows/definitions", h.ListWorkflowDefinitions)
	mux.HandleFunc("POST /v1/workflows/definitions", h.CreateWorkflowDefinition)
	mux.HandleFunc("GET /v1/workflows/definitions/{workflowType}", h.GetWorkflowDefinition)
	mux.HandleFunc("DELETE /v1/workflows/definitions/{workflowType}/{version}", h.DeleteWorkflowDefinition)
}

func (h *WorkflowHandler) CreateWorkflow(w http.ResponseWriter, r *http.Request) {
	var wf domain.WorkflowInstance
	if err := json.NewDecoder(r.Body).Decode(&wf); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	if wf.TenantID == "" {
		http.Error(w, `{"error":"tenant_id is required"}`, http.StatusBadRequest)
		return
	}
	if wf.WorkflowType == "" {
		http.Error(w, `{"error":"workflow_type is required"}`, http.StatusBadRequest)
		return
	}
	if wf.State == "" {
		wf.State = domain.WorkflowStatusPending
	}

	ctx := r.Context()
	if err := h.workflowSvc.Create(ctx, &wf); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(wf)
}

func (h *WorkflowHandler) GetWorkflow(w http.ResponseWriter, r *http.Request) {
	workflowID := r.PathValue("workflowID")
	if workflowID == "" {
		http.Error(w, `{"error":"workflowID is required"}`, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	wf, err := h.workflowSvc.Get(ctx, workflowID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if wf == nil {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(wf)
}

func (h *WorkflowHandler) TransitionWorkflow(w http.ResponseWriter, r *http.Request) {
	workflowID := r.PathValue("workflowID")
	if workflowID == "" {
		http.Error(w, `{"error":"workflowID is required"}`, http.StatusBadRequest)
		return
	}

	var body struct {
		FromState string `json:"from_state"`
		ToState   string `json:"to_state"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	if body.ToState == "" {
		http.Error(w, `{"error":"to_state is required"}`, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	var err error
	if body.FromState != "" {
		err = h.workflowSvc.Transition(ctx, workflowID, body.FromState, body.ToState)
	} else {
		err = h.workflowSvc.UpdateState(ctx, workflowID, body.ToState, 0)
	}

	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *WorkflowHandler) ListWorkflows(w http.ResponseWriter, r *http.Request) {
	tenantID := r.URL.Query().Get("tenant_id")
	state := r.URL.Query().Get("state")
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	if tenantID == "" {
		http.Error(w, `{"error":"tenant_id is required"}`, http.StatusBadRequest)
		return
	}

	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}
	offset := 0
	if offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil {
			offset = o
		}
	}

	ctx := r.Context()
	var workflows []*domain.WorkflowInstance
	var err error

	if state != "" {
		workflows, err = h.workflowSvc.GetByTenantAndState(ctx, tenantID, state, limit)
	} else {
		workflows, err = h.workflowSvc.GetByTenantAndState(ctx, tenantID, "", limit)
	}
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if offset > 0 && offset < len(workflows) {
		workflows = workflows[offset:]
	} else if offset >= len(workflows) {
		workflows = []*domain.WorkflowInstance{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"workflows": workflows,
		"total":     len(workflows),
		"limit":     limit,
		"offset":    offset,
	})
}

func (h *WorkflowHandler) ListWorkflowDefinitions(w http.ResponseWriter, r *http.Request) {
	tenantID := r.URL.Query().Get("tenant_id")

	ctx := r.Context()
	var defs []*domain.WorkflowDefinition
	var err error

	if tenantID != "" {
		defs, err = h.getWorkflowDefRepo().List(ctx, tenantID)
	} else {
		defs, err = h.getWorkflowDefRepo().List(ctx, "")
	}
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"definitions": defs,
		"total":       len(defs),
	})
}

func (h *WorkflowHandler) CreateWorkflowDefinition(w http.ResponseWriter, r *http.Request) {
	var def domain.WorkflowDefinition
	if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	if def.WorkflowType == "" {
		http.Error(w, `{"error":"workflow_type is required"}`, http.StatusBadRequest)
		return
	}
	if def.Version <= 0 {
		http.Error(w, `{"error":"version must be > 0"}`, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	if err := h.getWorkflowDefRepo().Save(ctx, &def); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(def)
}

func (h *WorkflowHandler) GetWorkflowDefinition(w http.ResponseWriter, r *http.Request) {
	workflowType := r.PathValue("workflowType")
	versionStr := r.PathValue("version")

	if workflowType == "" {
		http.Error(w, `{"error":"workflowType is required"}`, http.StatusBadRequest)
		return
	}

	var version int64 = 0
	if versionStr != "" {
		var err error
		version, err = strconv.ParseInt(versionStr, 10, 64)
		if err != nil {
			http.Error(w, `{"error":"invalid version"}`, http.StatusBadRequest)
			return
		}
	}

	ctx := r.Context()
	var def *domain.WorkflowDefinition
	var err error

	if version > 0 {
		def, err = h.getWorkflowDefRepo().Get(ctx, workflowType, version)
	} else {
		defs, err := h.getWorkflowDefRepo().List(ctx, "")
		if err == nil && len(defs) > 0 {
			def = defs[0]
		}
	}
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if def == nil {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(def)
}

func (h *WorkflowHandler) DeleteWorkflowDefinition(w http.ResponseWriter, r *http.Request) {
	workflowType := r.PathValue("workflowType")
	versionStr := r.PathValue("version")

	if workflowType == "" {
		http.Error(w, `{"error":"workflowType is required"}`, http.StatusBadRequest)
		return
	}

	version, err := strconv.ParseInt(versionStr, 10, 64)
	if err != nil {
		http.Error(w, `{"error":"invalid version"}`, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	if err := h.getWorkflowDefRepo().Delete(ctx, workflowType, version); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

func (h *WorkflowHandler) getWorkflowDefRepo() ports.WorkflowDefinitionRepository {
	return sql.NewWorkflowDefinitionRepository(h.pool.Pool())
}