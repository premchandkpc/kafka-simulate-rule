package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/flowrule/flowrule/internal/adapters/http"
	natsadapter "github.com/flowrule/flowrule/internal/adapters/nats"
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

func run() (runErr error) {
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

	natsCfg := natsadapter.Config{
		NatsURL:    cfg.NATS.URL,
		Stream:     cfg.NATS.Stream,
		Consumer:   cfg.NATS.Consumer,
		Subjects:   []string{"events"},
		AckWait:    cfg.NATSAckWait(),
		MaxDeliver: cfg.Worker.NATSMaxDeliver,
		NumShards:  cfg.Worker.NumShards,
	}
	publisher, err := natsadapter.NewConsumer(ctx, natsCfg)
	if err != nil {
		return fmt.Errorf("initialize NATS event publisher: %w", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close NATS event publisher: %w", err))
		}
	}()

	compiler := rules.NewCompiler(rules.DefaultLimits())
	repositories := db.Repositories()

	rulesSvc := svcrules.NewService(
		compiler,
		repositories.Rules,
		repositories.Activations,
		repositories.Executions,
		domain.SystemClock{},
		repositories.Transactions,
		repositories.NewRuleTxRepositories,
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

	router := http.NewRouterWithPublisher(
		rulesSvc,
		workflowSvc,
		workflowDefRepo,
		repositories.Contracts,
		batchSvc,
		quarantineSvc,
		repositories.ScheduledEvents,
		publisher,
		cfg.Worker.NumShards,
	)

	log.Printf("API server listening on %s", cfg.App.HTTPAddr)
	return router.Run(ctx, cfg.App.HTTPAddr)
}
