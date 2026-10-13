package handlers

import (
	"errors"
	"net/http"
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
		responses.WriteError(w, http.StatusForbidden, messageTenantMismatch, responses.CodeForbidden)
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
		responses.WriteError(w, http.StatusNotFound, messageNotFound, responses.CodeNotFound)
		return
	}

	responses.WriteOK(w, batch)
}

func (h *BatchHandler) ListBatches(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	ruleSet := r.URL.Query().Get(queryParamRuleSet)
	pagination, err := parsePagination(r, defaultPageLimit, maxPageLimit, maxPageOffset)
	if err != nil {
		responses.WriteError(w, http.StatusBadRequest, err.Error(), responses.CodeValidationFailed)
		return
	}
	limit, offset := pagination.Limit, pagination.Offset

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
		responses.WriteError(w, http.StatusUnauthorized, messageTenantRequired, responses.CodeTenantRequired)
		return
	}
	filter, err := buildQuarantineFilter(r, tenantID)
	if err != nil {
		responses.WriteError(w, http.StatusBadRequest, err.Error(), responses.CodeValidationFailed)
		return
	}
	entries, err := h.quarantineSvc.List(r.Context(), filter)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	responses.WriteOK(w, quarantineListResponse(entries, filter))
}

func buildQuarantineFilter(r *http.Request, tenantID string) (ports.QuarantineFilter, error) {
	query := r.URL.Query()
	filter := ports.QuarantineFilter{
		TenantID: tenantID, SourceType: query.Get(queryParamSourceType),
		ErrorClass: query.Get(queryParamErrorClass), Limit: 100,
	}
	from, to, err := parseQuarantineTimeRange(query.Get(queryParamFrom), query.Get(queryParamTo))
	if err != nil {
	    return ports.QuarantineFilter{}, err
	}
	filter.From, filter.To = from, to
	pagination, err := parsePagination(r, 100, 500, 10000)
	if err != nil {
	    return ports.QuarantineFilter{}, err
	}
	filter.Limit, filter.Offset = pagination.Limit, pagination.Offset
	return filter, nil
}

func parseQuarantineTimeRange(fromValue, toValue string) (time.Time, time.Time, error) {
	var from, to time.Time
	if fromValue != "" {
		parsed, err := time.Parse(time.RFC3339, fromValue)
		if err != nil {
		    return time.Time{}, time.Time{}, errors.New("invalid from timestamp; expected RFC3339")
		}
		from = parsed
	}
	if toValue != "" {
		parsed, err := time.Parse(time.RFC3339, toValue)
		if err != nil {
		    return time.Time{}, time.Time{}, errors.New("invalid to timestamp; expected RFC3339")
		}
		to = parsed
	}
	if !from.IsZero() && !to.IsZero() && from.After(to) {
		return time.Time{}, time.Time{}, errors.New("from must not be after to")
	}
	return from, to, nil
}

func quarantineListResponse(entries []*domain.QuarantineEntry, filter ports.QuarantineFilter) map[string]interface{} {
	return map[string]interface{}{
		"entries": entries, "total": len(entries), "limit": filter.Limit, "offset": filter.Offset,
	}
}

func (h *QuarantineHandler) GetQuarantine(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		responses.WriteError(w, http.StatusBadRequest, "id is required", responses.CodeBadRequest)
		return
	}

	ctx := r.Context()
	tenantID := middleware.GetTenantID(ctx)
	if tenantID == "" {
		responses.WriteError(w, http.StatusUnauthorized, "authenticated tenant identity is required", responses.CodeTenantRequired)
		return
	}
	entry, err := h.quarantineSvc.Get(ctx, tenantID, id)
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

	ctx := r.Context()
	tenantID := middleware.GetTenantID(ctx)
	if tenantID == "" {
		responses.WriteError(w, http.StatusUnauthorized, "authenticated tenant identity is required", responses.CodeTenantRequired)
		return
	}
	if err := h.quarantineSvc.Replay(ctx, tenantID, id); err != nil {
		if errors.Is(err, domain.ErrQuarantineNotFound) {
			responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
			return
		}
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
	tenantID := middleware.GetTenantID(ctx)
	if tenantID == "" {
		responses.WriteError(w, http.StatusUnauthorized, "authenticated tenant identity is required", responses.CodeTenantRequired)
		return
	}
	if err := h.quarantineSvc.Delete(ctx, tenantID, id); err != nil {
		if errors.Is(err, domain.ErrQuarantineNotFound) {
			responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
			return
		}
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	responses.WriteNoContent(w)
}
