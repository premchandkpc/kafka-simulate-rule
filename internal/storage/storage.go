package storage

import (
	"context"
	"fmt"
	"strings"

	mongoadapter "github.com/flowrule/flowrule/internal/adapters/mongo"
	sqladapter "github.com/flowrule/flowrule/internal/adapters/sql"
	"github.com/flowrule/flowrule/internal/config"
	"github.com/flowrule/flowrule/internal/ports"
)

type Repositories struct {
	Inbox                  ports.InboxRepository
	Activations            ports.ActivationRepository
	Rules                  ports.RuleRepository
	Executions             ports.ExecutionRepository
	Outbox                 ports.OutboxRepository
	ShardLeases            ports.ShardLeaseRepository
	Workflows              ports.WorkflowRepository
	WorkflowDefinitions    ports.WorkflowDefinitionRepository
	ScheduledEvents        ports.ScheduledEventRepository
	Batches                ports.BatchRepository
	Quarantine             ports.QuarantineRepository
	Contracts              ports.ContractRegistry
	Transactions           ports.TransactionManager
	NewEventTxRepositories ports.EventTxRepositoryFactory
	NewRuleTxRepositories  ports.RuleTxRepositoryFactory
}

type Storage struct {
	backend      string
	repositories Repositories
	initialize   func(context.Context) error
	close        func()
}

func Open(ctx context.Context, cfg config.DatabaseConfig) (*Storage, error) {
	backend := strings.ToLower(strings.TrimSpace(cfg.StorageBackend))
	if backend == "" {
		backend = "postgres"
	}

	switch backend {
	case "postgres":
		db, err := sqladapter.New(ctx, cfg.DSN, cfg.MigrationsDir)
		if err != nil {
			return nil, fmt.Errorf("open postgres storage: %w", err)
		}
		pool := db.Pool()
		return &Storage{
			backend: backend,
			repositories: Repositories{
				Inbox:                  sqladapter.NewInboxRepository(pool),
				Activations:            sqladapter.NewActivationRepository(pool),
				Rules:                  sqladapter.NewRuleRepository(pool),
				Executions:             sqladapter.NewExecutionRepository(pool),
				Outbox:                 sqladapter.NewOutboxRepository(pool),
				ShardLeases:            sqladapter.NewShardLeaseRepository(pool),
				Workflows:              sqladapter.NewWorkflowRepository(pool),
				WorkflowDefinitions:    sqladapter.NewWorkflowDefinitionRepository(pool),
				ScheduledEvents:        sqladapter.NewScheduledEventRepository(pool),
				Batches:                sqladapter.NewBatchRepository(pool),
				Quarantine:             sqladapter.NewQuarantineRepository(pool),
				Contracts:              sqladapter.NewContractRegistry(pool),
				Transactions:           sqladapter.NewTransactionManager(pool),
				NewEventTxRepositories: sqladapter.NewEventTxRepositories,
				NewRuleTxRepositories:  sqladapter.NewRuleTxRepositories,
			},
			initialize: db.RunMigrations,
			close:      db.Close,
		}, nil

	case "mongodb":
		db, err := mongoadapter.New(ctx, mongoadapter.Config{
			URI:      cfg.MongoDBURI,
			Database: cfg.MongoDBDatabase,
		})
		if err != nil {
			return nil, fmt.Errorf("open mongodb storage: %w", err)
		}
		q := mongoadapter.NewQuerier(db.Database())
		quarantineRepo := mongoadapter.NewQuarantineRepository(q)
		return &Storage{
			backend: backend,
			repositories: Repositories{
				Inbox:                  mongoadapter.NewInboxRepository(q),
				Activations:            mongoadapter.NewActivationRepository(q),
				Rules:                  mongoadapter.NewRepository(q),
				Executions:             mongoadapter.NewExecutionRepository(q),
				Outbox:                 mongoadapter.NewOutboxRepository(q),
				ShardLeases:            mongoadapter.NewShardLeaseRepository(q),
				Workflows:              mongoadapter.NewWorkflowRepository(q),
				WorkflowDefinitions:    mongoadapter.NewWorkflowDefinitionRepository(q),
				ScheduledEvents:        mongoadapter.NewScheduledEventRepository(q),
				Batches:                mongoadapter.NewBatchRepository(q),
				Quarantine:             quarantineRepo,
				Contracts:              mongoadapter.NewContractRegistry(q),
				Transactions:           mongoadapter.NewTransactionManager(db.Client(), db.Database()),
				NewEventTxRepositories: mongoadapter.NewEventTxRepositories,
				NewRuleTxRepositories:  mongoadapter.NewRuleTxRepositories,
			},
			initialize: quarantineRepo.BackfillEffectTenantIDs,
			close:      db.Close,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported storage backend %q", backend)
	}
}

func (s *Storage) Backend() string {
	return s.backend
}

func (s *Storage) Repositories() Repositories {
	return s.repositories
}

func (s *Storage) Initialize(ctx context.Context) error {
	return s.initialize(ctx)
}

func (s *Storage) Close() {
	s.close()
}
