package handlers

import (
	"net/http"
	"strconv"
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
	if evt.TenantID != "" && evt.TenantID != tenantID {
		responses.WriteError(w, http.StatusForbidden, "tenant_id must match the request tenant", responses.CodeForbidden)
		return
	}
	evt.TenantID = tenantID
	if evt.EventType == "" || evt.PartitionKey == "" {
		responses.WriteError(w, http.StatusBadRequest, "event_type and partition_key are required", responses.CodeValidationFailed)
		return
	}
	if evt.ScheduledAt.IsZero() {
		responses.WriteError(w, http.StatusBadRequest, "scheduled_at is required", responses.CodeValidationFailed)
		return
	}
	if evt.EventID == "" {
		evt.EventID = domain.NewID()
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

	evt, err := h.repo.Get(r.Context(), middleware.GetTenantID(r.Context()), eventID)
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
	limit := 100
	offset := 0
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 500 {
			responses.WriteError(w, http.StatusBadRequest, "limit must be an integer between 1 and 500", responses.CodeValidationFailed)
			return
		}
		limit = parsed
	}
	if value := r.URL.Query().Get("offset"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			responses.WriteError(w, http.StatusBadRequest, "offset must be a non-negative integer", responses.CodeValidationFailed)
			return
		}
		offset = parsed
	}

	events, err := h.repo.List(r.Context(), middleware.GetTenantID(r.Context()), limit, offset)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	responses.WriteOK(w, map[string]interface{}{
		"events": events,
		"total":  len(events),
		"limit":  limit,
		"offset": offset,
	})
}

func (h *ScheduledHandler) CancelScheduledEvent(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("eventID")
	if eventID == "" {
		responses.WriteError(w, http.StatusBadRequest, "eventID is required", responses.CodeBadRequest)
		return
	}
	if err := h.repo.Cancel(r.Context(), middleware.GetTenantID(r.Context()), eventID); err != nil {
		responses.WriteError(w, http.StatusConflict, "event was not found for this tenant or is no longer pending", responses.CodeConflict)
		return
	}
	responses.WriteNoContent(w)
}
