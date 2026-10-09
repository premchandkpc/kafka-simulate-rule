package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type ScheduledHandler struct {
	repo ports.ScheduledEventRepository
}

func NewScheduledHandler(repo ports.ScheduledEventRepository) *ScheduledHandler {
	return &ScheduledHandler{repo: repo}
}

func (h *ScheduledHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/scheduled-events", h.ScheduleEvent)
	mux.HandleFunc("GET /v1/scheduled-events/{eventID}", h.GetScheduledEvent)
	mux.HandleFunc("GET /v1/scheduled-events", h.ListScheduledEvents)
	mux.HandleFunc("DELETE /v1/scheduled-events/{eventID}", h.CancelScheduledEvent)
}

func (h *ScheduledHandler) ScheduleEvent(w http.ResponseWriter, r *http.Request) {
	var evt domain.ScheduledEvent
	if err := decodeRequestBody(r, &evt); err != nil {
		http.Error(w, `{"error":"invalid request body: `+err.Error()+`"}`, http.StatusBadRequest)
		return
	}

	if evt.EventID == "" {
		evt.EventID = domain.NewID()
	}
	if evt.TenantID == "" || evt.EventType == "" || evt.PartitionKey == "" {
		http.Error(w, `{"error":"tenant_id, event_type, and partition_key are required"}`, http.StatusBadRequest)
		return
	}
	if evt.ScheduledAt.IsZero() {
		http.Error(w, `{"error":"scheduled_at is required"}`, http.StatusBadRequest)
		return
	}

	evt.Status = "pending"
	evt.CreatedAt = time.Now().UTC()

	ctx := r.Context()
	if err := h.repo.Save(ctx, &evt); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(evt); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *ScheduledHandler) GetScheduledEvent(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("eventID")
	if eventID == "" {
		http.Error(w, `{"error":"eventID is required"}`, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	evt, err := h.repo.Get(ctx, eventID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if evt == nil {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(evt); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *ScheduledHandler) ListScheduledEvents(w http.ResponseWriter, r *http.Request) {
	// This is a simplified list - would need a proper List method in the repository
	// For now, return empty
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"events": []interface{}{},
		"total":  0,
	}); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *ScheduledHandler) CancelScheduledEvent(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("eventID")
	if eventID == "" {
		http.Error(w, `{"error":"eventID is required"}`, http.StatusBadRequest)
		return
	}

	// Mark as failed/cancelled
	ctx := r.Context()
	if err := h.repo.MarkFailed(ctx, eventID, "cancelled by user"); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}