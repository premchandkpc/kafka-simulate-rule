package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/flowrule/flowrule/cmd/api/handlers"
	"github.com/flowrule/flowrule/internal/config"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/rules"
	svcbatches "github.com/flowrule/flowrule/internal/services/batches"
	svcquarantine "github.com/flowrule/flowrule/internal/services/quarantine"
	svcrules "github.com/flowrule/flowrule/internal/services/rules"
	svcworkflow "github.com/flowrule/flowrule/internal/services/workflow"
	"github.com/flowrule/flowrule/internal/storage"
)

// apiError represents a standardized API error response
type apiError struct {
	Error string `json:"error"`
	Code  string `json:"code,omitempty"`
}

func writeError(w http.ResponseWriter, status int, message string, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiError{Error: message, Code: code})
}

// getTenantScope extracts tenant scope from request.
// In production, this should come from authenticated principal.
func getTenantScope(r *http.Request) string {
	// Check for explicit tenant header (for multi-tenant testing)
	if tenant := r.Header.Get("X-Tenant-ID"); tenant != "" {
		return tenant
	}
	// Check query param
	if tenant := r.URL.Query().Get("tenant_id"); tenant != "" {
		return tenant
	}
	// Default for single-tenant deployments
	return "default"
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	db, err := storage.Open(ctx, cfg.Database)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer db.Close()

	if err := db.Initialize(ctx); err != nil {
		log.Fatalf("migrations: %v", err)
	}

	compiler := rules.NewCompiler(rules.DefaultLimits())
	clock := domain.SystemClock{}
	repositories := db.Repositories()

	rulesSvc := svcrules.NewService(
		compiler,
		repositories.Rules,
		repositories.Activations,
		repositories.Executions,
		clock,
	)

	workflowSvc := svcworkflow.NewService(
		repositories.Workflows,
		clock,
	)

	batchSvc := svcbatches.NewService(
		repositories.Batches,
		nil, // events processor - not needed for query API
		clock,
		domain.BatchConfig{Mode: domain.BatchModeNone},
	)

	quarantineSvc := svcquarantine.NewService(repositories.Quarantine)

	workflowDefRepo := repositories.WorkflowDefinitions

	mux := http.NewServeMux()

	// Rule endpoints
	mux.HandleFunc("POST /v1/rules/{ruleSet}/revisions", func(w http.ResponseWriter, r *http.Request) {
		ruleSet := r.PathValue("ruleSet")
		var body json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json", "INVALID_JSON")
			return
		}

		tenantScope := getTenantScope(r)
		activation, err := rulesSvc.Activate(r.Context(), tenantScope, ruleSet, body, "api")
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "ACTIVATION_FAILED")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(activation); err != nil {
			log.Printf("encode response: %v", err)
		}
	})

	mux.HandleFunc("POST /v1/rules/{ruleSet}/revisions/{revision}/activate", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"not implemented"}`, http.StatusNotImplemented)
	})

	mux.HandleFunc("GET /v1/executions/{executionID}", func(w http.ResponseWriter, r *http.Request) {
		execID := r.PathValue("executionID")
		exec, err := rulesSvc.GetExecution(r.Context(), execID)
		if err != nil || exec == nil {
			writeError(w, http.StatusNotFound, "not found", "NOT_FOUND")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(exec); err != nil {
			log.Printf("encode response: %v", err)
		}
	})

	// Rule extension endpoints
	ruleHandler := handlers.NewRuleHandler(rulesSvc)
	ruleHandler.RegisterRoutes(mux)

	// Workflow instance endpoints
	mux.HandleFunc("POST /v1/workflows", func(w http.ResponseWriter, r *http.Request) {
		var workflow domain.WorkflowInstance
		if err := json.NewDecoder(r.Body).Decode(&workflow); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json", "INVALID_JSON")
			return
		}

		// If tenant_id not provided in body, use from request context
		if workflow.TenantID == "" {
			workflow.TenantID = getTenantScope(r)
		}
		if workflow.TenantID == "" {
			writeError(w, http.StatusBadRequest, "tenant_id is required", "TENANT_REQUIRED")
			return
		}
		if workflow.WorkflowType == "" {
			writeError(w, http.StatusBadRequest, "workflow_type is required", "WORKFLOW_TYPE_REQUIRED")
			return
		}

		if err := workflowSvc.Create(r.Context(), &workflow); err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "CREATE_FAILED")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(workflow); err != nil {
			log.Printf("encode response: %v", err)
		}
	})

	mux.HandleFunc("GET /v1/workflows/{workflowID}", func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("workflowID")
		workflow, err := workflowSvc.Get(r.Context(), workflowID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error(), "INTERNAL_ERROR")
			return
		}
		if workflow == nil {
			writeError(w, http.StatusNotFound, "not found", "NOT_FOUND")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(workflow); err != nil {
			log.Printf("encode response: %v", err)
		}
	})

	mux.HandleFunc("POST /v1/workflows/{workflowID}/transition", func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("workflowID")
		var req struct {
			FromState string `json:"from_state"`
			ToState   string `json:"to_state"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json", "INVALID_JSON")
			return
		}

		if req.FromState == "" || req.ToState == "" {
			writeError(w, http.StatusBadRequest, "from_state and to_state are required", "INVALID_TRANSITION")
			return
		}

		if err := workflowSvc.Transition(r.Context(), workflowID, req.FromState, req.ToState); err != nil {
			writeError(w, http.StatusBadRequest, err.Error(), "TRANSITION_FAILED")
			return
		}

		workflow, _ := workflowSvc.Get(r.Context(), workflowID)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(workflow); err != nil {
			log.Printf("encode response: %v", err)
		}
	})

	mux.HandleFunc("GET /v1/workflows", func(w http.ResponseWriter, r *http.Request) {
		tenantID := getTenantScope(r)
		state := r.URL.Query().Get("state")
		limitStr := r.URL.Query().Get("limit")

		limit := 100
		if limitStr != "" {
			if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
				limit = l
			}
		}

		workflows, err := workflowSvc.GetByTenantAndState(r.Context(), tenantID, state, limit)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error(), "INTERNAL_ERROR")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(workflows); err != nil {
			log.Printf("encode response: %v", err)
		}
	})

	// Workflow definition endpoints
	mux.HandleFunc("POST /v1/workflow-definitions", func(w http.ResponseWriter, r *http.Request) {
		var def domain.WorkflowDefinition
		if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json", "INVALID_JSON")
			return
		}

		if def.WorkflowType == "" {
			writeError(w, http.StatusBadRequest, "workflow_type is required", "WORKFLOW_TYPE_REQUIRED")
			return
		}
		if def.Version <= 0 {
			writeError(w, http.StatusBadRequest, "version must be > 0", "INVALID_VERSION")
			return
		}

		now := clock.Now()
		def.CreatedAt = now
		def.UpdatedAt = now

		if err := workflowDefRepo.Save(r.Context(), &def); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error(), "SAVE_FAILED")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(def); err != nil {
			log.Printf("encode response: %v", err)
		}
	})

	mux.HandleFunc("GET /v1/workflow-definitions/{workflowType}", func(w http.ResponseWriter, r *http.Request) {
		workflowType := r.PathValue("workflowType")
		versionStr := r.URL.Query().Get("version")

		var def *domain.WorkflowDefinition
		var err error

		if versionStr != "" {
			version, parseErr := strconv.ParseInt(versionStr, 10, 64)
			if parseErr != nil {
				writeError(w, http.StatusBadRequest, "invalid version", "INVALID_VERSION")
				return
			}
			def, err = workflowDefRepo.Get(r.Context(), workflowType, version)
		} else {
			defs, listErr := workflowDefRepo.List(r.Context(), "")
			if listErr != nil {
				writeError(w, http.StatusInternalServerError, listErr.Error(), "INTERNAL_ERROR")
				return
			}
			// Return latest version
			for _, d := range defs {
				if d.WorkflowType == workflowType {
					def = d
					break
				}
			}
		}

		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error(), "INTERNAL_ERROR")
			return
		}
		if def == nil {
			writeError(w, http.StatusNotFound, "not found", "NOT_FOUND")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(def); err != nil {
			log.Printf("encode response: %v", err)
		}
	})

	mux.HandleFunc("GET /v1/workflow-definitions", func(w http.ResponseWriter, r *http.Request) {
		defs, err := workflowDefRepo.List(r.Context(), "")
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error(), "INTERNAL_ERROR")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(defs); err != nil {
			log.Printf("encode response: %v", err)
		}
	})

	// Contract endpoints
	contractHandler := handlers.NewContractHandler(repositories.Contracts)
	mux.HandleFunc("POST /v1/contracts", contractHandler.CreateContract)
	mux.HandleFunc("GET /v1/contracts", contractHandler.ListContracts)
	mux.HandleFunc("GET /v1/contracts/{name}", contractHandler.GetContract)
	mux.HandleFunc("GET /v1/contracts/{name}/{version}", contractHandler.GetContractVersion)
	mux.HandleFunc("DELETE /v1/contracts/{name}/{version}", contractHandler.DeleteContract)
	mux.HandleFunc("POST /v1/contracts/{name}/{version}/generate", contractHandler.GenerateContract)

	// Batch endpoints
	batchHandler := handlers.NewBatchHandler(batchSvc)
	batchHandler.RegisterRoutes(mux)

	// Quarantine endpoints
	quarantineHandler := handlers.NewQuarantineHandler(quarantineSvc)
	quarantineHandler.RegisterRoutes(mux)

	// Scheduled event endpoints
	scheduledHandler := handlers.NewScheduledHandler(repositories.ScheduledEvents)
	scheduledHandler.RegisterRoutes(mux)

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]string{"status": "ok"}); err != nil {
			log.Printf("health write: %v", err)
		}
	})

	server := &http.Server{
		Addr:         cfg.App.HTTPAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("API server listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.App.ShutdownTimeout)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown: %v", err)
	}
}
