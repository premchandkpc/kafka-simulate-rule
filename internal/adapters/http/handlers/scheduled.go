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

	tenantID := middleware.GetTenantID(r.Context())
	if tenantID == "" {
		responses.WriteError(w, http.StatusUnauthorized, "authenticated tenant identity is required", responses.CodeTenantRequired)
		return
	}
	if evt.TenantID != "" && evt.TenantID != tenantID {
		responses.WriteError(w, http.StatusForbidden, "tenant_id does not match request tenant", responses.CodeValidationFailed)
		return
	}
	evt.TenantID = tenantID

	if evt.EventID == "" {
		evt.EventID = domain.NewID()
	}
	if evt.EventType == "" || evt.PartitionKey == "" {
		responses.WriteError(w, http.StatusBadRequest, "event_type and partition_key are required", responses.CodeValidationFailed)
		return
	}
	if evt.ScheduledAt.IsZero() {
		responses.WriteError(w, http.StatusBadRequest, "scheduled_at is required", responses.CodeValidationFailed)
		return
	}
	if !evt.ScheduledAt.After(time.Now().UTC()) {
		responses.WriteError(w, http.StatusBadRequest, "scheduled_at must be in the future", responses.CodeValidationFailed)
		return
	}

	evt.Status = "pending"
	evt.CreatedAt = time.Now().UTC()
	if err := h.repo.Save(r.Context(), &evt); err != nil {
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

	evt, err := h.repo.Get(r.Context(), eventID)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if evt == nil || evt.TenantID != middleware.GetTenantID(r.Context()) {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}
	responses.WriteOK(w, evt)
}

func (h *ScheduledHandler) ListScheduledEvents(w http.ResponseWriter, r *http.Request) {
	if middleware.GetTenantID(r.Context()) == "" {
		responses.WriteError(w, http.StatusUnauthorized, "authenticated tenant identity is required", responses.CodeTenantRequired)
		return
	}
	// The repository has no tenant-scoped list operation yet. Do not report a
	// successful empty result for data the API cannot actually query.
	responses.WriteError(w, http.StatusNotImplemented, "scheduled event listing is not implemented", responses.CodeInternalError)
}

func (h *ScheduledHandler) CancelScheduledEvent(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("eventID")
	if eventID == "" {
		responses.WriteError(w, http.StatusBadRequest, "eventID is required", responses.CodeBadRequest)
		return
	}

	evt, err := h.repo.Get(r.Context(), eventID)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if evt == nil || evt.TenantID != middleware.GetTenantID(r.Context()) {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}

	// MarkFailed cannot represent a cancellation state. Returning 501 is safer
	// than silently changing the event to failed while claiming it was cancelled.
	responses.WriteError(w, http.StatusNotImplemented, "scheduled event cancellation is not implemented by the repository", responses.CodeInternalError)
}
