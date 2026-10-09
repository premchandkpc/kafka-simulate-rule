package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

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
		TenantID     string   `json:"tenant_id"`
		PartitionKey string   `json:"partition_key"`
		RuleSet      string   `json:"rule_set"`
		EventIDs     []string `json:"event_ids"`
	}
	if err := decodeRequestBody(r, &req); err != nil {
		http.Error(w, `{"error":"invalid request body: `+err.Error()+`"}`, http.StatusBadRequest)
		return
	}

	if req.TenantID == "" || req.PartitionKey == "" || req.RuleSet == "" {
		http.Error(w, `{"error":"tenant_id, partition_key, and rule_set are required"}`, http.StatusBadRequest)
		return
	}
	if len(req.EventIDs) == 0 {
		http.Error(w, `{"error":"at least one event_id is required"}`, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	batch, err := h.batchSvc.CreateBatch(ctx, req.TenantID, req.PartitionKey, req.RuleSet, req.EventIDs)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(batch); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *BatchHandler) GetBatch(w http.ResponseWriter, r *http.Request) {
	batchID := r.PathValue("batchID")
	if batchID == "" {
		http.Error(w, `{"error":"batchID is required"}`, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	batch, err := h.batchSvc.GetBatch(ctx, batchID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if batch == nil {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(batch); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *BatchHandler) ListBatches(w http.ResponseWriter, r *http.Request) {
	tenantID := r.URL.Query().Get("tenant_id")
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
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"batches": batches,
		"total":   len(batches),
		"limit":   limit,
		"offset":  offset,
	}); err != nil {
		log.Printf("encode response: %v", err)
	}
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
		TenantID:   r.URL.Query().Get("tenant_id"),
		SourceType: r.URL.Query().Get("source_type"),
		ErrorClass: r.URL.Query().Get("error_class"),
	}

	if fromStr := r.URL.Query().Get("from"); fromStr != "" {
		// Parse time - support RFC3339
		// For simplicity, we'll skip time parsing in this handler
	}

	if toStr := r.URL.Query().Get("to"); toStr != "" {
		// Parse time
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
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"entries": entries,
		"total":   len(entries),
		"limit":   filter.Limit,
		"offset":  filter.Offset,
	}); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *QuarantineHandler) GetQuarantine(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, `{"error":"id is required"}`, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	entry, err := h.quarantineSvc.Get(ctx, id)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if entry == nil {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(entry); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *QuarantineHandler) ReplayQuarantine(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, `{"error":"id is required"}`, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	if err := h.quarantineSvc.Replay(ctx, id); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "replayed"}); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *QuarantineHandler) DeleteQuarantine(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, `{"error":"id is required"}`, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	if err := h.quarantineSvc.Delete(ctx, id); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
