package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/flowrule/flowrule/cmd/api/handlers"
	"github.com/flowrule/flowrule/internal/adapters/sql"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/rules"
	svcrules "github.com/flowrule/flowrule/internal/services/rules"
	svcworkflow "github.com/flowrule/flowrule/internal/services/workflow"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/flowrule?sslmode=disable"
	}
	migrationsDir := os.Getenv("MIGRATIONS_DIR")
	if migrationsDir == "" {
		migrationsDir = "migrations"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	db, err := sql.New(ctx, dsn, migrationsDir)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer db.Close()

	if err := db.RunMigrations(ctx); err != nil {
		log.Fatalf("migrations: %v", err)
	}

	compiler := rules.NewCompiler(rules.DefaultLimits())
	clock := domain.SystemClock{}
	pool := db.Pool()

	rulesSvc := svcrules.NewService(
		compiler,
		sql.NewRuleRepository(pool),
		sql.NewActivationRepository(pool),
		sql.NewExecutionRepository(pool),
		clock,
	)

	workflowSvc := svcworkflow.NewService(
		sql.NewWorkflowRepository(pool),
		clock,
	)

	workflowDefRepo := sql.NewWorkflowDefinitionRepository(pool)

	mux := http.NewServeMux()

	// Rule endpoints
	mux.HandleFunc("POST /v1/rules/{ruleSet}/revisions", func(w http.ResponseWriter, r *http.Request) {
		ruleSet := r.PathValue("ruleSet")
		var body json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
			return
		}

		activation, err := rulesSvc.Activate(r.Context(), "default", ruleSet, body, "api")
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(activation)
	})

	mux.HandleFunc("POST /v1/rules/{ruleSet}/revisions/{revision}/activate", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"not implemented"}`, http.StatusNotImplemented)
	})

	mux.HandleFunc("GET /v1/executions/{executionID}", func(w http.ResponseWriter, r *http.Request) {
		execID := r.PathValue("executionID")
		exec, err := rulesSvc.GetExecution(r.Context(), execID)
		if err != nil || exec == nil {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(exec)
	})

	// Workflow instance endpoints
	mux.HandleFunc("POST /v1/workflows", func(w http.ResponseWriter, r *http.Request) {
		var workflow domain.WorkflowInstance
		if err := json.NewDecoder(r.Body).Decode(&workflow); err != nil {
			http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
			return
		}

		if workflow.TenantID == "" {
			http.Error(w, `{"error":"tenant_id is required"}`, http.StatusBadRequest)
			return
		}
		if workflow.WorkflowType == "" {
			http.Error(w, `{"error":"workflow_type is required"}`, http.StatusBadRequest)
			return
		}

		if err := workflowSvc.Create(r.Context(), &workflow); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(workflow)
	})

	mux.HandleFunc("GET /v1/workflows/{workflowID}", func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("workflowID")
		workflow, err := workflowSvc.Get(r.Context(), workflowID)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		if workflow == nil {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(workflow)
	})

	mux.HandleFunc("POST /v1/workflows/{workflowID}/transition", func(w http.ResponseWriter, r *http.Request) {
		workflowID := r.PathValue("workflowID")
		var req struct {
			FromState string `json:"from_state"`
			ToState   string `json:"to_state"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
			return
		}

		if req.FromState == "" || req.ToState == "" {
			http.Error(w, `{"error":"from_state and to_state are required"}`, http.StatusBadRequest)
			return
		}

		if err := workflowSvc.Transition(r.Context(), workflowID, req.FromState, req.ToState); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
			return
		}

		workflow, _ := workflowSvc.Get(r.Context(), workflowID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(workflow)
	})

	mux.HandleFunc("GET /v1/workflows", func(w http.ResponseWriter, r *http.Request) {
		tenantID := r.URL.Query().Get("tenant_id")
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
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(workflows)
	})

	// Workflow definition endpoints
	mux.HandleFunc("POST /v1/workflow-definitions", func(w http.ResponseWriter, r *http.Request) {
		var def domain.WorkflowDefinition
		if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
			http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
			return
		}

		if def.WorkflowType == "" {
			http.Error(w, `{"error":"workflow_type is required"}`, http.StatusBadRequest)
			return
		}
		if def.Version <= 0 {
			http.Error(w, `{"error":"version must be > 0"}`, http.StatusBadRequest)
			return
		}

		now := clock.Now()
		def.CreatedAt = now
		def.UpdatedAt = now

		if err := workflowDefRepo.Save(r.Context(), &def); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(def)
	})

	mux.HandleFunc("GET /v1/workflow-definitions/{workflowType}", func(w http.ResponseWriter, r *http.Request) {
		workflowType := r.PathValue("workflowType")
		versionStr := r.URL.Query().Get("version")

		var def *domain.WorkflowDefinition
		var err error

		if versionStr != "" {
			version, parseErr := strconv.ParseInt(versionStr, 10, 64)
			if parseErr != nil {
				http.Error(w, `{"error":"invalid version"}`, http.StatusBadRequest)
				return
			}
			def, err = workflowDefRepo.Get(r.Context(), workflowType, version)
		} else {
			defs, listErr := workflowDefRepo.List(r.Context(), "")
			if listErr != nil {
				http.Error(w, fmt.Sprintf(`{"error":"%s"}`, listErr.Error()), http.StatusInternalServerError)
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
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		if def == nil {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(def)
	})

	mux.HandleFunc("GET /v1/workflow-definitions", func(w http.ResponseWriter, r *http.Request) {
		defs, err := workflowDefRepo.List(r.Context(), "")
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(defs)
	})

	// Contract endpoints
	contractHandler := handlers.NewContractHandler(db)
	mux.HandleFunc("POST /v1/contracts", contractHandler.CreateContract)
	mux.HandleFunc("GET /v1/contracts", contractHandler.ListContracts)
	mux.HandleFunc("GET /v1/contracts/{name}", contractHandler.GetContract)
	mux.HandleFunc("GET /v1/contracts/{name}/{version}", contractHandler.GetContractVersion)
	mux.HandleFunc("DELETE /v1/contracts/{name}/{version}", contractHandler.DeleteContract)
	mux.HandleFunc("POST /v1/contracts/{name}/{version}/generate", contractHandler.GenerateContract)

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	server := &http.Server{
		Addr:         ":8080",
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
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	server.Shutdown(shutdownCtx)
}
