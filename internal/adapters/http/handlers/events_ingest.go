package handlers

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/flowrule/flowrule/internal/adapters/http/middleware"
	"github.com/flowrule/flowrule/internal/adapters/http/responses"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type EventIngestionHandler struct {
	publisher ports.BrokerPublisher
	numShards uint32
}

func NewEventIngestionHandler(publisher ports.BrokerPublisher, numShards uint32) *EventIngestionHandler {
	if numShards == 0 {
		numShards = 4096
	}
	return &EventIngestionHandler{publisher: publisher, numShards: numShards}
}

func (h *EventIngestionHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/events/{eventType}", h.PublishEvent)
}

func (h *EventIngestionHandler) PublishEvent(w http.ResponseWriter, r *http.Request) {
	if h.publisher == nil {
		responses.WriteError(w, http.StatusServiceUnavailable, "event ingestion is not configured", responses.CodeInternalError)
		return
	}

	var req struct {
		ID            string            `json:"id"`
		Type          string            `json:"type"`
		TenantID      string            `json:"tenant_id"`
		PartitionKey  string            `json:"partition_key"`
		WorkflowID    string            `json:"workflow_id,omitempty"`
		OccurredAt    time.Time         `json:"occurred_at"`
		Data          []byte            `json:"data"`
		Headers       map[string]string `json:"headers,omitempty"`
		CorrelationID string            `json:"correlation_id,omitempty"`
		CausationID   string            `json:"causation_id,omitempty"`
		SchemaVersion string            `json:"schema_version,omitempty"`
	}
	if err := decodeRequestBody(r, &req); err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid event envelope: "+err.Error(), responses.CodeInvalidJSON)
		return
	}

	eventType := strings.TrimSpace(r.PathValue("eventType"))
	if eventType == "" || req.Type != eventType {
		responses.WriteError(w, http.StatusBadRequest, "event type must match the eventType path parameter", responses.CodeValidationFailed)
		return
	}
	tenantID := middleware.GetTenantID(r.Context())
	if tenantID == "" {
		responses.WriteError(w, http.StatusUnauthorized, "authenticated tenant identity is required", responses.CodeTenantRequired)
		return
	}
	if req.TenantID != "" && req.TenantID != tenantID {
		responses.WriteError(w, http.StatusForbidden, "tenant_id does not match request tenant", responses.CodeForbidden)
		return
	}
	if len(req.Data) == 0 || !json.Valid(req.Data) {
		responses.WriteError(w, http.StatusBadRequest, "data must contain a valid JSON value", responses.CodeValidationFailed)
		return
	}

	event := &domain.EventEnvelope{
		ID:           strings.TrimSpace(req.ID),
		Type:         eventType,
		TenantID:     tenantID,
		PartitionKey: strings.TrimSpace(req.PartitionKey),
		WorkflowID:   req.WorkflowID,
		OccurredAt:   req.OccurredAt,
		Data:         json.RawMessage(req.Data),
		Headers:      req.Headers,
	}
	if err := event.Validate(); err != nil {
		responses.WriteError(w, http.StatusBadRequest, err.Error(), responses.CodeValidationFailed)
		return
	}

	payload, err := json.Marshal(event)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, "failed to encode event envelope", responses.CodeInternalError)
		return
	}
	shard := event.VirtualShard(h.numShards)
	if err := h.publisher.PublishToShard(r.Context(), "events", shard, payload); err != nil {
		responses.WriteError(w, http.StatusServiceUnavailable, "event publication failed", responses.CodeInternalError)
		return
	}

	responses.WriteSuccess(w, http.StatusAccepted, map[string]interface{}{
		"event_id": event.ID,
		"status":   "accepted",
		"shard":    shard,
	})
}
