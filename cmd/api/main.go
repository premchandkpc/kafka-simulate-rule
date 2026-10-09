package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/flowrule/flowrule/internal/adapters/http"
	"github.com/flowrule/flowrule/internal/config"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/rules"
	svcbatches "github.com/flowrule/flowrule/internal/services/batches"
	svcquarantine "github.com/flowrule/flowrule/internal/services/quarantine"
	svcrules "github.com/flowrule/flowrule/internal/services/rules"
	svcworkflow "github.com/flowrule/flowrule/internal/services/workflow"
	"github.com/flowrule/flowrule/internal/storage"
)

func main() {
	if err := run(); err != nil {
		log.Printf("api stopped: %v", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	db, err := storage.Open(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Initialize(ctx); err != nil {
		return err
	}

	compiler := rules.NewCompiler(rules.DefaultLimits())
	repositories := db.Repositories()

	rulesSvc := svcrules.NewService(
		compiler,
		repositories.Rules,
		repositories.Activations,
		repositories.Executions,
		domain.SystemClock{},
	)

	workflowSvc := svcworkflow.NewService(
		repositories.Workflows,
		domain.SystemClock{},
	)

	batchSvc := svcbatches.NewService(
		repositories.Batches,
		nil, // events processor - not needed for query API
		domain.SystemClock{},
		domain.BatchConfig{Mode: domain.BatchModeNone},
	)

	quarantineSvc := svcquarantine.NewService(repositories.Quarantine)

	workflowDefRepo := repositories.WorkflowDefinitions

	router := http.NewRouter(
		rulesSvc,
		workflowSvc,
		workflowDefRepo,
		repositories.Contracts,
		batchSvc,
		quarantineSvc,
		repositories.ScheduledEvents,
	)

	log.Printf("API server listening on %s", cfg.App.HTTPAddr)
	return router.Run(ctx, cfg.App.HTTPAddr)
}
