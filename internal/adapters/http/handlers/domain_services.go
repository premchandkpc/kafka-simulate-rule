package handlers

import (
	"errors"
	"net/http"

	"github.com/flowrule/flowrule/internal/adapters/http/middleware"
	"github.com/flowrule/flowrule/internal/adapters/http/responses"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/flowrule/flowrule/internal/services/domainservices"
)

type DomainServiceHandler struct {
	service ports.DomainServiceUseCase
}

func NewDomainServiceHandler(service ports.DomainServiceUseCase) *DomainServiceHandler {
	return &DomainServiceHandler{service: service}
}

func (h *DomainServiceHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/domains", h.ListDomains)
	mux.HandleFunc("POST /v1/domains", h.CreateDomain)
	mux.HandleFunc("GET /v1/domains/{domainID}", h.GetDomain)
	mux.HandleFunc("PUT /v1/domains/{domainID}", h.UpdateDomain)
	mux.HandleFunc("DELETE /v1/domains/{domainID}", h.DeleteDomain)
	mux.HandleFunc("GET /v1/services", h.ListServices)
	mux.HandleFunc("POST /v1/services", h.CreateService)
	mux.HandleFunc("GET /v1/services/{serviceID}", h.GetService)
	mux.HandleFunc("PUT /v1/services/{serviceID}", h.UpdateService)
	mux.HandleFunc("DELETE /v1/services/{serviceID}", h.DeleteService)
}

func (h *DomainServiceHandler) ListDomains(w http.ResponseWriter, r *http.Request) {
	if !h.ready(w, r) {
		return
	}
	items, err := h.service.ListDomains(r.Context(), middleware.GetTenantID(r.Context()))
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteOK(w, map[string]any{"domains": items, "total": len(items)})
}

func (h *DomainServiceHandler) CreateDomain(w http.ResponseWriter, r *http.Request) {
	var item domain.TenantDomain
	if !h.decode(w, r, &item) {
		return
	}
	if !h.ready(w, r) {
		return
	}
	saved, err := h.service.SaveDomain(r.Context(), middleware.GetTenantID(r.Context()), &item)
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteCreated(w, saved)
}

func (h *DomainServiceHandler) GetDomain(w http.ResponseWriter, r *http.Request) {
	if !h.ready(w, r) {
		return
	}
	item, err := h.service.GetDomain(r.Context(), middleware.GetTenantID(r.Context()), r.PathValue("domainID"))
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteOK(w, item)
}

func (h *DomainServiceHandler) UpdateDomain(w http.ResponseWriter, r *http.Request) {
	var item domain.TenantDomain
	if !h.decode(w, r, &item) {
		return
	}
	if !h.ready(w, r) {
		return
	}
	item.ID = r.PathValue("domainID")
	saved, err := h.service.SaveDomain(r.Context(), middleware.GetTenantID(r.Context()), &item)
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteOK(w, saved)
}

func (h *DomainServiceHandler) DeleteDomain(w http.ResponseWriter, r *http.Request) {
	if !h.ready(w, r) {
		return
	}
	if err := h.service.DeleteDomain(r.Context(), middleware.GetTenantID(r.Context()), r.PathValue("domainID")); err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteNoContent(w)
}

func (h *DomainServiceHandler) ListServices(w http.ResponseWriter, r *http.Request) {
	if !h.ready(w, r) {
		return
	}
	items, err := h.service.ListServices(r.Context(), middleware.GetTenantID(r.Context()))
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteOK(w, map[string]any{"services": items, "total": len(items)})
}

func (h *DomainServiceHandler) CreateService(w http.ResponseWriter, r *http.Request) {
	var item domain.DomainService
	if !h.decode(w, r, &item) {
		return
	}
	if !h.ready(w, r) {
		return
	}
	saved, err := h.service.SaveService(r.Context(), middleware.GetTenantID(r.Context()), &item)
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteCreated(w, saved)
}

func (h *DomainServiceHandler) GetService(w http.ResponseWriter, r *http.Request) {
	if !h.ready(w, r) {
		return
	}
	item, err := h.service.GetService(r.Context(), middleware.GetTenantID(r.Context()), r.PathValue("serviceID"))
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteOK(w, item)
}

func (h *DomainServiceHandler) UpdateService(w http.ResponseWriter, r *http.Request) {
	var item domain.DomainService
	if !h.decode(w, r, &item) {
		return
	}
	if !h.ready(w, r) {
		return
	}
	item.ID = r.PathValue("serviceID")
	saved, err := h.service.SaveService(r.Context(), middleware.GetTenantID(r.Context()), &item)
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteOK(w, saved)
}

func (h *DomainServiceHandler) DeleteService(w http.ResponseWriter, r *http.Request) {
	if !h.ready(w, r) {
		return
	}
	if err := h.service.DeleteService(r.Context(), middleware.GetTenantID(r.Context()), r.PathValue("serviceID")); err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteNoContent(w)
}

func (h *DomainServiceHandler) ready(w http.ResponseWriter, r *http.Request) bool {
	if h.service == nil {
		responses.WriteError(w, http.StatusServiceUnavailable, "domain and service management is not configured", responses.CodeInternalError)
		return false
	}
	if middleware.GetTenantID(r.Context()) == "" {
		responses.WriteError(w, http.StatusUnauthorized, "authenticated tenant identity is required", responses.CodeTenantRequired)
		return false
	}
	return true
}

func (h *DomainServiceHandler) decode(w http.ResponseWriter, r *http.Request, item any) bool {
	if err := decodeRequestBody(r, item); err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), responses.CodeInvalidJSON)
		return false
	}
	return true
}

func writeDomainServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domainservices.ErrInvalidInput):
		responses.WriteError(w, http.StatusBadRequest, err.Error(), responses.CodeValidationFailed)
	case errors.Is(err, domainservices.ErrConflict):
		responses.WriteError(w, http.StatusConflict, err.Error(), responses.CodeConflict)
	case errors.Is(err, domainservices.ErrNotFound):
		responses.WriteError(w, http.StatusNotFound, err.Error(), responses.CodeNotFound)
	default:
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
	}
}
