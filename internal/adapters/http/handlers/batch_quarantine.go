package handlers

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/flowrule/flowrule/internal/adapters/http/middleware"
	"github.com/flowrule/flowrule/internal/adapters/http/responses"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type BatchHandler struct {
	batchSvc ports.BatchService
}

func NewBatchHandler(batchSvc ports.BatchService) *BatchHandler {
	return &BatchHandler{batchSvc: batchSvc}
}

func (h *BatchHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/batches", h.CreateBatch)
	mux.HandleFunc("GET /v1/batches/{batchID}", h.GetBatch)
	mux.HandleFunc("GET /v1/batches", h.ListBatches)
}

func (h *BatchHandler) CreateBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TenantID     string   `json:"tenant_id"`
		PartitionKey string   `json:"partition_key"`
		RuleSet      string   `json:"rule_set"`
		EventIDs     []string `json:"event_ids"`
	}
	if err := decodeRequestBody(r, &req); err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), responses.CodeInvalidJSON)
		return
	}

	tenantID := middleware.GetTenantID(r.Context())
	if req.TenantID != "" && req.TenantID != tenantID {
		responses.WriteError(w, http.StatusForbidden, "tenant_id must match the request tenant", responses.CodeForbidden)
		return
	}
	req.TenantID = tenantID
	if req.TenantID == "" || req.PartitionKey == "" || req.RuleSet == "" {
		responses.WriteError(w, http.StatusBadRequest, "tenant_id, partition_key, and rule_set are required", responses.CodeValidationFailed)
		return
	}
	if len(req.EventIDs) == 0 {
		responses.WriteError(w, http.StatusBadRequest, "at least one event_id is required", responses.CodeValidationFailed)
		return
	}

	ctx := r.Context()
	batch, err := h.batchSvc.CreateBatch(ctx, req.TenantID, req.PartitionKey, req.RuleSet, req.EventIDs)
	if err != nil {
		if errors.Is(err, domain.ErrBatchProcessingUnavailable) {
			responses.WriteError(w, http.StatusServiceUnavailable, "batch processing is not configured for this API process", responses.CodeInternalError)
			return
		}
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteCreated(w, batch)
}

func (h *BatchHandler) GetBatch(w http.ResponseWriter, r *http.Request) {
	batchID := r.PathValue("batchID")
	if batchID == "" {
		responses.WriteError(w, http.StatusBadRequest, "batchID is required", responses.CodeBadRequest)
		return
	}

	ctx := r.Context()
	batch, err := h.batchSvc.GetBatch(ctx, middleware.GetTenantID(ctx), batchID)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if batch == nil {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}

	responses.WriteOK(w, batch)
}

func (h *BatchHandler) ListBatches(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	ruleSet := r.URL.Query().Get("rule_set")
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	limit := 100
	if limitStr != "" {
		l, err := strconv.Atoi(limitStr)
		if err != nil || l < 1 || l > 500 {
			responses.WriteError(w, http.StatusBadRequest, "limit must be an integer between 1 and 500", responses.CodeValidationFailed)
			return
		}
		limit = l
	}
	offset := 0
	if offsetStr != "" {
		o, err := strconv.Atoi(offsetStr)
		if err != nil || o < 0 {
			responses.WriteError(w, http.StatusBadRequest, "offset must be a non-negative integer", responses.CodeValidationFailed)
			return
		}
		offset = o
	}

	ctx := r.Context()
	batches, err := h.batchSvc.ListBatches(ctx, tenantID, ruleSet, limit, offset)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteOK(w, map[string]interface{}{
		"batches": batches,
		"total":   len(batches),
		"limit":   limit,
		"offset":  offset,
	})
}

type QuarantineHandler struct {
	quarantineSvc ports.QuarantineService
}

func NewQuarantineHandler(quarantineSvc ports.QuarantineService) *QuarantineHandler {
	return &QuarantineHandler{quarantineSvc: quarantineSvc}
}

func (h *QuarantineHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/quarantine", h.ListQuarantine)
	mux.HandleFunc("GET /v1/quarantine/{id}", h.GetQuarantine)
	mux.HandleFunc("POST /v1/quarantine/{id}/replay", h.ReplayQuarantine)
	mux.HandleFunc("DELETE /v1/quarantine/{id}", h.DeleteQuarantine)
}

func (h *QuarantineHandler) ListQuarantine(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	if tenantID == "" {
		responses.WriteError(w, http.StatusUnauthorized, "authenticated tenant identity is required", responses.CodeTenantRequired)
		return
	}

	filter := ports.QuarantineFilter{
		TenantID:   tenantID,
		SourceType: r.URL.Query().Get("source_type"),
		ErrorClass: r.URL.Query().Get("error_class"),
		Limit:      100,
	}
	if value := r.URL.Query().Get("from"); value != "" {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, "invalid from timestamp; expected RFC3339", responses.CodeValidationFailed)
			return
		}
		filter.From = parsed
	}
	if value := r.URL.Query().Get("to"); value != "" {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, "invalid to timestamp; expected RFC3339", responses.CodeValidationFailed)
			return
		}
		filter.To = parsed
	}
	if !filter.From.IsZero() && !filter.To.IsZero() && filter.From.After(filter.To) {
		responses.WriteError(w, http.StatusBadRequest, "from must not be after to", responses.CodeValidationFailed)
		return
	}
	if value := r.URL.Query().Get("limit"); value != "" {
		limit, err := strconv.Atoi(value)
		if err != nil || limit < 1 || limit > 500 {
			responses.WriteError(w, http.StatusBadRequest, "limit must be an integer between 1 and 500", responses.CodeValidationFailed)
			return
		}
		filter.Limit = limit
	}
	if value := r.URL.Query().Get("offset"); value != "" {
		offset, err := strconv.Atoi(value)
		if err != nil || offset < 0 {
			responses.WriteError(w, http.StatusBadRequest, "offset must be a non-negative integer", responses.CodeValidationFailed)
			return
		}
		filter.Offset = offset
	}

	entries, err := h.quarantineSvc.List(r.Context(), filter)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	responses.WriteOK(w, map[string]interface{}{
		"entries": entries,
		"total":   len(entries),
		"limit":   filter.Limit,
		"offset":  filter.Offset,
	})
}
func (h *QuarantineHandler) GetQuarantine(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		responses.WriteError(w, http.StatusBadRequest, "id is required", responses.CodeBadRequest)
		return
	}

	ctx := r.Context()
	entry, err := h.quarantineSvc.Get(ctx, id)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if entry == nil || entry.TenantID != middleware.GetTenantID(r.Context()) {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}

	responses.WriteOK(w, entry)
}

func (h *QuarantineHandler) ReplayQuarantine(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		responses.WriteError(w, http.StatusBadRequest, "id is required", responses.CodeBadRequest)
		return
	}

	ctx := r.Context()
	entry, err := h.quarantineSvc.Get(ctx, id)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if entry == nil || entry.TenantID != middleware.GetTenantID(ctx) {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}
	if err := h.quarantineSvc.Replay(ctx, id); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteOK(w, map[string]string{"status": "replayed"})
}

func (h *QuarantineHandler) DeleteQuarantine(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		responses.WriteError(w, http.StatusBadRequest, "id is required", responses.CodeBadRequest)
		return
	}

	ctx := r.Context()
	entry, err := h.quarantineSvc.Get(ctx, id)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if entry == nil || entry.TenantID != middleware.GetTenantID(ctx) {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}
	if err := h.quarantineSvc.Delete(ctx, id); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteNoContent(w)
}
