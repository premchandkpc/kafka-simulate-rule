package handlers

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/flowrule/flowrule/internal/adapters/http/middleware"
	"github.com/flowrule/flowrule/internal/adapters/http/responses"
	"github.com/flowrule/flowrule/internal/ports"
)

const (
	defaultPageSize = 100
	maxPageSize     = 1000
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
		responses.WriteError(w, http.StatusForbidden, "tenant_id does not match request tenant", responses.CodeValidationFailed)
		return
	}
	if tenantID == "" {
		responses.WriteError(w, http.StatusUnauthorized, "tenant identity is required", responses.CodeTenantRequired)
		return
	}
	if req.PartitionKey == "" || req.RuleSet == "" {
		responses.WriteError(w, http.StatusBadRequest, "partition_key and rule_set are required", responses.CodeValidationFailed)
		return
	}
	if len(req.EventIDs) == 0 {
		responses.WriteError(w, http.StatusBadRequest, "at least one event_id is required", responses.CodeValidationFailed)
		return
	}

	batch, err := h.batchSvc.CreateBatch(r.Context(), tenantID, req.PartitionKey, req.RuleSet, req.EventIDs)
	if err != nil {
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

	batch, err := h.batchSvc.GetBatch(r.Context(), batchID)
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
	if tenantID == "" {
		responses.WriteError(w, http.StatusUnauthorized, "tenant identity is required", responses.CodeTenantRequired)
		return
	}

	limit, err := parsePageSize(r.URL.Query().Get("limit"))
	if err != nil {
		responses.WriteError(w, http.StatusBadRequest, err.Error(), responses.CodeValidationFailed)
		return
	}
	offset, err := parseOffset(r.URL.Query().Get("offset"))
	if err != nil {
		responses.WriteError(w, http.StatusBadRequest, err.Error(), responses.CodeValidationFailed)
		return
	}

	batches, err := h.batchSvc.ListBatches(r.Context(), tenantID, r.URL.Query().Get("rule_set"), limit, offset)
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
		responses.WriteError(w, http.StatusUnauthorized, "tenant identity is required", responses.CodeTenantRequired)
		return
	}

	filter := ports.QuarantineFilter{
		TenantID:   tenantID,
		SourceType: r.URL.Query().Get("source_type"),
		ErrorClass: r.URL.Query().Get("error_class"),
		Limit:      defaultPageSize,
	}
	for key, dst := range map[string]*time.Time{"from": &filter.From, "to": &filter.To} {
		if value := r.URL.Query().Get(key); value != "" {
			parsed, err := time.Parse(time.RFC3339, value)
			if err != nil {
				responses.WriteError(w, http.StatusBadRequest, "invalid "+key+" timestamp; expected RFC3339", responses.CodeValidationFailed)
				return
			}
			*dst = parsed
		}
	}
	if value := r.URL.Query().Get("limit"); value != "" {
		limit, err := strconv.Atoi(value)
		if err != nil || limit < 1 || limit > maxPageSize {
			responses.WriteError(w, http.StatusBadRequest, "limit must be between 1 and 1000", responses.CodeValidationFailed)
			return
		}
		filter.Limit = limit
	}
	if value := r.URL.Query().Get("offset"); value != "" {
		offset, err := strconv.Atoi(value)
		if err != nil || offset < 0 {
			responses.WriteError(w, http.StatusBadRequest, "offset must be non-negative", responses.CodeValidationFailed)
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
	entry, err := h.quarantineSvc.Get(r.Context(), id)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if entry == nil {
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
	if err := h.quarantineSvc.Replay(r.Context(), id); err != nil {
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
	if err := h.quarantineSvc.Delete(r.Context(), id); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	responses.WriteNoContent(w)
}

func parsePageSize(value string) (int, error) {
	if value == "" {
		return defaultPageSize, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 || limit > maxPageSize {
		return 0, fmt.Errorf("limit must be between 1 and %d", maxPageSize)
	}
	return limit, nil
}

func parseOffset(value string) (int, error) {
	if value == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(value)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("offset must be non-negative")
	}
	return offset, nil
}
