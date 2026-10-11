package handlers

import (
	"net/http"
	"strconv"

	"github.com/flowrule/flowrule/internal/adapters/http/middleware"
	"github.com/flowrule/flowrule/internal/adapters/http/responses"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type WorkflowHandler struct {
	workflowSvc     ports.WorkflowService
	ruleSvc         ports.RuleService
	workflowDefRepo ports.WorkflowDefinitionRepository
}

func NewWorkflowHandler(workflowSvc ports.WorkflowService, ruleSvc ports.RuleService, workflowDefRepo ports.WorkflowDefinitionRepository) *WorkflowHandler {
	return &WorkflowHandler{
		workflowSvc:     workflowSvc,
		ruleSvc:         ruleSvc,
		workflowDefRepo: workflowDefRepo,
	}
}

func (h *WorkflowHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/workflows", h.CreateWorkflow)
	mux.HandleFunc("GET /v1/workflows/{workflowID}", h.GetWorkflow)
	mux.HandleFunc("POST /v1/workflows/{workflowID}/transition", h.TransitionWorkflow)
	mux.HandleFunc("GET /v1/workflows", h.ListWorkflows)
	mux.HandleFunc("GET /v1/workflows/definitions", h.ListWorkflowDefinitions)
	mux.HandleFunc("GET /v1/workflows/definitions/{workflowType}", h.GetWorkflowDefinition)
}

func (h *WorkflowHandler) CreateWorkflow(w http.ResponseWriter, r *http.Request) {
	var wf domain.WorkflowInstance
	if err := decodeRequestBody(r, &wf); err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), responses.CodeInvalidJSON)
		return
	}

	tenantID := middleware.GetTenantID(r.Context())
	if wf.TenantID != "" && wf.TenantID != tenantID {
		responses.WriteError(w, http.StatusForbidden, "tenant_id must match the request tenant", responses.CodeForbidden)
		return
	}
	wf.TenantID = tenantID
	if wf.TenantID == "" {
		responses.WriteError(w, http.StatusBadRequest, "tenant_id is required", responses.CodeTenantRequired)
		return
	}
	if wf.WorkflowType == "" {
		responses.WriteError(w, http.StatusBadRequest, "workflow_type is required", responses.CodeWorkflowTypeReq)
		return
	}
	if wf.State == "" {
		wf.State = domain.WorkflowStatusPending
	}

	ctx := r.Context()
	if err := h.workflowSvc.Create(ctx, &wf); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteCreated(w, wf)
}

func (h *WorkflowHandler) GetWorkflow(w http.ResponseWriter, r *http.Request) {
	workflowID := r.PathValue("workflowID")
	if workflowID == "" {
		responses.WriteError(w, http.StatusBadRequest, "workflowID is required", responses.CodeBadRequest)
		return
	}

	ctx := r.Context()
	wf, err := h.workflowSvc.Get(ctx, workflowID)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if wf == nil || wf.TenantID != middleware.GetTenantID(r.Context()) {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}

	responses.WriteOK(w, wf)
}

func (h *WorkflowHandler) TransitionWorkflow(w http.ResponseWriter, r *http.Request) {
	workflowID := r.PathValue("workflowID")
	if workflowID == "" {
		responses.WriteError(w, http.StatusBadRequest, "workflowID is required", responses.CodeBadRequest)
		return
	}

	var body struct {
		FromState string `json:"from_state"`
		ToState   string `json:"to_state"`
	}
	if err := decodeRequestBody(r, &body); err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), responses.CodeInvalidJSON)
		return
	}

	if body.ToState == "" {
		responses.WriteError(w, http.StatusBadRequest, "to_state is required", responses.CodeBadRequest)
		return
	}

	ctx := r.Context()
	wf, err := h.workflowSvc.Get(ctx, workflowID)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if wf == nil || wf.TenantID != middleware.GetTenantID(ctx) {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}
	if body.FromState != "" && body.FromState != wf.State {
		responses.WriteError(w, http.StatusConflict, "from_state does not match current workflow state", responses.CodeConflict)
		return
	}
	if body.FromState != "" {
		err = h.workflowSvc.Transition(ctx, workflowID, body.FromState, body.ToState)
	} else {
		err = h.workflowSvc.UpdateState(ctx, workflowID, body.ToState, 0)
	}

	if err != nil {
		responses.WriteError(w, http.StatusBadRequest, err.Error(), responses.CodeTransitionFailed)
		return
	}

	responses.WriteOK(w, map[string]string{"status": "ok"})
}

func (h *WorkflowHandler) ListWorkflows(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	state := r.URL.Query().Get("state")
	pagination, err := parsePagination(r, 100, 1000, 10000)
	if err != nil {
		responses.WriteError(w, http.StatusBadRequest, err.Error(), responses.CodeValidationFailed)
		return
	}
	limit, offset := pagination.Limit, pagination.Offset

	ctx := r.Context()
	var workflows []*domain.WorkflowInstance

	fetchLimit := limit + offset
	if state != "" {
		workflows, err = h.workflowSvc.GetByTenantAndState(ctx, tenantID, state, fetchLimit)
	} else {
		workflows, err = h.workflowSvc.GetByTenantAndState(ctx, tenantID, "", fetchLimit)
	}
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	if offset > 0 && offset < len(workflows) {
		workflows = workflows[offset:]
	} else if offset >= len(workflows) {
		workflows = []*domain.WorkflowInstance{}
	}

	responses.WriteOK(w, map[string]interface{}{
		"workflows": workflows,
		"total":     len(workflows),
		"limit":     limit,
		"offset":    offset,
	})
}

func (h *WorkflowHandler) ListWorkflowDefinitions(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())

	ctx := r.Context()
	defs, err := h.workflowDefRepo.List(ctx, tenantID)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteOK(w, map[string]interface{}{
		"definitions": defs,
		"total":       len(defs),
	})
}

// CreateWorkflowDefinition is intentionally not registered on the tenant-facing router.
// Definitions are global platform configuration until a separately authorized admin API
// and an explicit ownership model are introduced.
func (h *WorkflowHandler) CreateWorkflowDefinition(w http.ResponseWriter, r *http.Request) {
	var def domain.WorkflowDefinition
	if err := decodeRequestBody(r, &def); err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), responses.CodeInvalidJSON)
		return
	}

	if def.WorkflowType == "" {
		responses.WriteError(w, http.StatusBadRequest, "workflow_type is required", responses.CodeWorkflowTypeReq)
		return
	}
	if def.Version <= 0 {
		responses.WriteError(w, http.StatusBadRequest, "version must be > 0", responses.CodeInvalidVersion)
		return
	}

	ctx := r.Context()
	if err := h.workflowDefRepo.Save(ctx, &def); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeSaveFailed)
		return
	}

	responses.WriteCreated(w, def)
}

func (h *WorkflowHandler) GetWorkflowDefinition(w http.ResponseWriter, r *http.Request) {
	workflowType := r.PathValue("workflowType")
	versionStr := r.PathValue("version")

	if workflowType == "" {
		responses.WriteError(w, http.StatusBadRequest, "workflowType is required", responses.CodeBadRequest)
		return
	}

	var version int64 = 0
	if versionStr != "" {
		var err error
		version, err = strconv.ParseInt(versionStr, 10, 64)
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, "invalid version", responses.CodeInvalidVersion)
			return
		}
	}

	ctx := r.Context()
	var def *domain.WorkflowDefinition
	var err error

	if version > 0 {
		def, err = h.workflowDefRepo.Get(ctx, workflowType, version)
	} else {
		defs, listErr := h.workflowDefRepo.List(ctx, "")
		if listErr != nil {
			err = listErr
		} else {
			for _, candidate := range defs {
				if candidate.WorkflowType == workflowType && (def == nil || candidate.Version > def.Version) {
					def = candidate
				}
			}
		}
	}
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if def == nil {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}

	responses.WriteOK(w, def)
}

// DeleteWorkflowDefinition is intentionally not registered on the tenant-facing router.
func (h *WorkflowHandler) DeleteWorkflowDefinition(w http.ResponseWriter, r *http.Request) {
	workflowType := r.PathValue("workflowType")
	versionStr := r.PathValue("version")

	if workflowType == "" {
		responses.WriteError(w, http.StatusBadRequest, "workflowType is required", responses.CodeBadRequest)
		return
	}

	version, err := strconv.ParseInt(versionStr, 10, 64)
	if err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid version", responses.CodeInvalidVersion)
		return
	}

	ctx := r.Context()
	if err := h.workflowDefRepo.Delete(ctx, workflowType, version); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeDeleteFailed)
		return
	}

	responses.WriteOK(w, map[string]string{"status": "deleted"})
}
