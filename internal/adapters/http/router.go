package http

import (
	"context"
	"net/http"
	"time"

	"github.com/flowrule/flowrule/internal/adapters/http/handlers"
	"github.com/flowrule/flowrule/internal/adapters/http/middleware"
	"github.com/flowrule/flowrule/internal/adapters/http/responses"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/flowrule/flowrule/internal/services/batches"
	"github.com/flowrule/flowrule/internal/services/quarantine"
	"github.com/flowrule/flowrule/internal/services/rules"
	"github.com/flowrule/flowrule/internal/services/workflow"
)

// Router holds all HTTP handlers and middleware
type Router struct {
	handler http.Handler
}

// NewRouter creates a new HTTP router with all routes registered
func NewRouter(
	ruleSvc *rules.Service,
	workflowSvc *workflow.Service,
	workflowDefRepo ports.WorkflowDefinitionRepository,
	contractRegistry ports.ContractRegistry,
	batchSvc *batches.Service,
	quarantineSvc *quarantine.Service,
	scheduledRepo ports.ScheduledEventRepository,
) *Router {
	mux := http.NewServeMux()

	// Apply middleware chain
	handler := middleware.Recovery(mux)
	handler = middleware.TenantExtractor(handler)

	// Health endpoint
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		responses.WriteOK(w, map[string]string{"status": "ok"})
	})

	// Rule handlers
	ruleHandler := handlers.NewRuleHandler(ruleSvc)
	ruleHandler.RegisterRoutes(mux)

	// Workflow handlers
	workflowHandler := handlers.NewWorkflowHandler(workflowSvc, nil, workflowDefRepo)
	workflowHandler.RegisterRoutes(mux)

	// Contract handlers
	contractHandler := handlers.NewContractHandler(contractRegistry)
	contractHandler.RegisterRoutes(mux)

	// Batch handlers
	batchHandler := handlers.NewBatchHandler(batchSvc)
	batchHandler.RegisterRoutes(mux)

	// Quarantine handlers
	quarantineHandler := handlers.NewQuarantineHandler(quarantineSvc)
	quarantineHandler.RegisterRoutes(mux)

	// Scheduled event handlers
	scheduledHandler := handlers.NewScheduledHandler(scheduledRepo)
	scheduledHandler.RegisterRoutes(mux)

	return &Router{handler: handler}
}

// ServeHTTP implements http.Handler
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.handler.ServeHTTP(w, req)
}

// Run starts the HTTP server
func (r *Router) Run(ctx context.Context, addr string) error {
	server := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in a goroutine
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ListenAndServe()
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-serverErr:
		return err
	}
}
