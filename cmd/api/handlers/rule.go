package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/flowrule/flowrule/internal/domain"
	svcrules "github.com/flowrule/flowrule/internal/services/rules"
)

type RuleHandler struct {
	ruleSvc *svcrules.Service
}

func NewRuleHandler(ruleSvc *svcrules.Service) *RuleHandler {
	return &RuleHandler{ruleSvc: ruleSvc}
}

func (h *RuleHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/rules/{ruleSet}/revisions", h.ListRevisions)
	mux.HandleFunc("GET /v1/rules/{ruleSet}/revisions/{revision}", h.GetRevision)
	mux.HandleFunc("DELETE /v1/rules/{ruleSet}/revisions/{revision}", h.Deactivate)
	mux.HandleFunc("GET /v1/rules/active", h.ListActiveRules)
}

func (h *RuleHandler) ListRevisions(w http.ResponseWriter, r *http.Request) {
	ruleSet := r.PathValue("ruleSet")
	tenantScope := r.URL.Query().Get("tenant_scope")
	if tenantScope == "" {
		tenantScope = "default"
	}

	ctx := r.Context()
	revisions, err := h.ruleSvc.ListRevisions(ctx, tenantScope)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	// Filter by ruleSet if provided
	if ruleSet != "" {
		filtered := make([]*domain.RuleRevision, 0)
		for _, rev := range revisions {
			if rev.RuleID == ruleSet {
				filtered = append(filtered, rev)
			}
		}
		revisions = filtered
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"revisions": revisions,
		"total":     len(revisions),
	}); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *RuleHandler) GetRevision(w http.ResponseWriter, r *http.Request) {
	ruleSet := r.PathValue("ruleSet")
	revisionStr := r.PathValue("revision")
	tenantScope := r.URL.Query().Get("tenant_scope")
	if tenantScope == "" {
		tenantScope = "default"
	}

	revision, err := strconv.ParseInt(revisionStr, 10, 64)
	if err != nil {
		http.Error(w, `{"error":"invalid revision"}`, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	rev, err := h.ruleSvc.GetRevision(ctx, tenantScope, ruleSet, revision)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if rev == nil {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(rev); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *RuleHandler) Deactivate(w http.ResponseWriter, r *http.Request) {
	ruleSet := r.PathValue("ruleSet")
	tenantScope := r.URL.Query().Get("tenant_scope")
	if tenantScope == "" {
		tenantScope = "default"
	}

	ctx := r.Context()
	if err := h.ruleSvc.Deactivate(ctx, tenantScope, ruleSet); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "deactivated"}); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func (h *RuleHandler) ListActiveRules(w http.ResponseWriter, r *http.Request) {
	tenantScope := r.URL.Query().Get("tenant_scope")
	if tenantScope == "" {
		tenantScope = "default"
	}

	ctx := r.Context()
	active, err := h.ruleSvc.ListActiveRules(ctx, tenantScope)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"active_rules": active,
		"total":        len(active),
	}); err != nil {
		log.Printf("encode response: %v", err)
	}
}
