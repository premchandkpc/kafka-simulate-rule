package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/flowrule/flowrule/internal/adapters/http/middleware"
	"github.com/flowrule/flowrule/internal/adapters/http/responses"
	"github.com/flowrule/flowrule/internal/ports"
	svcbatches "github.com/flowrule/flowrule/internal/services/batches"
	svcquarantine "github.com/flowrule/flowrule/internal/services/quarantine"
)

type BatchHandler struct {
	batchSvc *svcbatches.Service
}

func NewBatchHandler(batchSvc *svcbatches.Service) *BatchHandler {
	return &BatchHandler{batchSvc: batchSvc}
}

func (h *BatchHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/batches", h.CreateBatch)
	mux.HandleFunc("GET /v1/batches/{batchID}", h.GetBatch)
	mux.HandleFunc("GET /v1/batches", h.ListBatches)
}

func (h *BatchHandler) CreateBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TenantID      string   `json:"tenant_id"`
		PartitionKey  string   `json:"partition_key"`
		RuleSet       string   `json:"rule_set"`
		EventIDs      []string `json:"event_ids"`
	}
	if err := decodeRequestBody(r, &req); err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), responses.CodeInvalidJSON)
		return
	}

	// Use tenant from middleware if not provided
	if req.TenantID == "" {
		req.TenantID = middleware.GetTenantID(r.Context())
	}
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
	batch, err := h.batchSvc.GetBatch(ctx, batchID)
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
	quarantineSvc *svcquarantine.Service
}

func NewQuarantineHandler(quarantineSvc *svcquarantine.Service) *QuarantineHandler {
	return &QuarantineHandler{quarantineSvc: quarantineSvc}
}

func (h *QuarantineHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/quarantine", h.ListQuarantine)
	mux.HandleFunc("GET /v1/quarantine/{id}", h.GetQuarantine)
	mux.HandleFunc("POST /v1/quarantine/{id}/replay", h.ReplayQuarantine)
	mux.HandleFunc("DELETE /v1/quarantine/{id}", h.DeleteQuarantine)
}

func (h *QuarantineHandler) ListQuarantine(w http.ResponseWriter, r *http.Request) {
	filter := ports.QuarantineFilter{
		TenantID:   middleware.GetTenantID(r.Context()),
		SourceType: r.URL.Query().Get("source_type"),
		ErrorClass: r.URL.Query().Get("error_class"),
	}

	// Parse time filters if provided
	if fromStr := r.URL.Query().Get("from"); fromStr != "" {
		if t, err := time.Parse(time.RFC3339, fromStr); err == nil {
			filter.From = t
		}
	}
	if toStr := r.URL.Query().Get("to"); toStr != "" {
		if t, err := time.Parse(time.RFC3339, toStr); err == nil {
			filter.To = t
		}
	}

	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			filter.Limit = l
		}
	}
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	if offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil {
			filter.Offset = o
		}
	}

	ctx := r.Context()
	entries, err := h.quarantineSvc.List(ctx, filter)
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
	if err := h.quarantineSvc.Delete(ctx, id); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteNoContent(w)
}

// decodeRequestBody decodes the request body as JSON
func decodeRequestBody(r *http.Request, v interface{}) error {
	return json.NewDecoder(r.Body).Decode(v)
}