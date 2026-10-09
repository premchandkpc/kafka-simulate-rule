package handlers

import (
	"net/http"
	"time"

	"github.com/flowrule/flowrule/internal/adapters/http/middleware"
	"github.com/flowrule/flowrule/internal/adapters/http/responses"
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
		responses.WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), responses.CodeInvalidJSON)
		return
	}

	if evt.EventID == "" {
		evt.EventID = domain.NewID()
	}
	// Use tenant from middleware if not provided
	if evt.TenantID == "" {
		evt.TenantID = middleware.GetTenantID(r.Context())
	}
	if evt.TenantID == "" || evt.EventType == "" || evt.PartitionKey == "" {
		responses.WriteError(w, http.StatusBadRequest, "tenant_id, event_type, and partition_key are required", responses.CodeValidationFailed)
		return
	}
	if evt.ScheduledAt.IsZero() {
		responses.WriteError(w, http.StatusBadRequest, "scheduled_at is required", responses.CodeValidationFailed)
		return
	}

	evt.Status = "pending"
	evt.CreatedAt = time.Now().UTC()

	ctx := r.Context()
	if err := h.repo.Save(ctx, &evt); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteCreated(w, evt)
}

func (h *ScheduledHandler) GetScheduledEvent(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("eventID")
	if eventID == "" {
		responses.WriteError(w, http.StatusBadRequest, "eventID is required", responses.CodeBadRequest)
		return
	}

	ctx := r.Context()
	evt, err := h.repo.Get(ctx, eventID)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if evt == nil {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}

	responses.WriteOK(w, evt)
}

func (h *ScheduledHandler) ListScheduledEvents(w http.ResponseWriter, r *http.Request) {
	// This requires a List method in the repository interface
	// For now, return empty with proper response format
	responses.WriteOK(w, map[string]interface{}{
		"events": []interface{}{},
		"total":  0,
	})
}

func (h *ScheduledHandler) CancelScheduledEvent(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("eventID")
	if eventID == "" {
		responses.WriteError(w, http.StatusBadRequest, "eventID is required", responses.CodeBadRequest)
		return
	}

	// Mark as failed/cancelled
	ctx := r.Context()
	if err := h.repo.MarkFailed(ctx, eventID, "cancelled by user"); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteNoContent(w)
}
