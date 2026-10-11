package storage

import (
	"context"
	"fmt"
	"strings"

	mongoadapter "github.com/flowrule/flowrule/internal/adapters/mongo"
	sqladapter "github.com/flowrule/flowrule/internal/adapters/sql"
	"github.com/flowrule/flowrule/internal/config"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repositories struct {
	Inbox               ports.InboxRepository
	Activations         ports.ActivationRepository
	Rules               ports.RuleRepository
	Executions          ports.ExecutionRepository
	Outbox              ports.OutboxRepository
	ShardLeases         ports.ShardLeaseRepository
	Workflows           ports.WorkflowRepository
	WorkflowDefinitions ports.WorkflowDefinitionRepository
	ScheduledEvents     ports.ScheduledEventRepository
	Batches             ports.BatchRepository
	Quarantine          ports.QuarantineRepository
	Contracts           ports.ContractRegistry
	Transactions        ports.TransactionManager
	NewEventTxRepositories ports.EventTxRepositoryFactory
	NewRuleTxRepositories  ports.RuleTxRepositoryFactory
}

type Storage struct {
	backend      string
	repositories Repositories
	initialize   func(context.Context) error
	close        func()
}

// Backend returns the storage backend name
func (s *Storage) Backend() string {
	return s.backend
}

// Repositories returns the repository instances
func (s *Storage) Repositories() Repositories {
	return s.repositories
}

// Initialize initializes the storage (runs migrations, etc.)
func (s *Storage) Initialize(ctx context.Context) error {
	return s.initialize(ctx)
}

// Close closes the storage connection
func (s *Storage) Close() {
	s.close()
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
			backend:      backend,
			repositories: buildPostgresRepos(pool, db),
			initialize:   db.RunMigrations,
			close:        db.Close,
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
		repositories, initialize := buildMongoRepos(q, db)
		return &Storage{
			backend:      backend,
			repositories: repositories,
			initialize:   initialize,
			close:        db.Close,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported storage backend %q", backend)
	}
}

func buildPostgresRepos(pool *pgxpool.Pool, _ *sqladapter.DB) Repositories {
	return Repositories{
		Inbox:               sqladapter.NewInboxRepository(pool),
		Activations:         sqladapter.NewActivationRepository(pool),
		Rules:               sqladapter.NewRuleRepository(pool),
		Executions:          sqladapter.NewExecutionRepository(pool),
		Outbox:              sqladapter.NewOutboxRepository(pool),
		ShardLeases:         sqladapter.NewShardLeaseRepository(pool),
		Workflows:           sqladapter.NewWorkflowRepository(pool),
		WorkflowDefinitions: sqladapter.NewWorkflowDefinitionRepository(pool),
		ScheduledEvents:     sqladapter.NewScheduledEventRepository(pool),
		Batches:             sqladapter.NewBatchRepository(pool),
		Quarantine:          sqladapter.NewQuarantineRepository(pool),
		Contracts:           sqladapter.NewContractRegistry(pool),
		Transactions:        sqladapter.NewTransactionManager(pool),
		NewEventTxRepositories: sqladapter.NewEventTxRepositories,
		NewRuleTxRepositories:  sqladapter.NewRuleTxRepositories,
	}
}

func buildMongoRepos(q mongoadapter.Querier, db *mongoadapter.DB) (Repositories, func(context.Context) error) {
	quarantineRepo := mongoadapter.NewQuarantineRepository(q)
	return Repositories{
		Inbox:               mongoadapter.NewInboxRepository(q),
		Activations:         mongoadapter.NewActivationRepository(q),
		Rules:               mongoadapter.NewRuleRepository(q),
		Executions:          mongoadapter.NewExecutionRepository(q),
		Outbox:              mongoadapter.NewOutboxRepository(q),
		ShardLeases:         mongoadapter.NewShardLeaseRepository(q),
		Workflows:           mongoadapter.NewWorkflowRepository(q),
		WorkflowDefinitions: mongoadapter.NewWorkflowDefinitionRepository(q),
		ScheduledEvents:     mongoadapter.NewScheduledEventRepository(q),
		Batches:             mongoadapter.NewBatchRepository(q),
		Quarantine:          quarantineRepo,
		Contracts:           mongoadapter.NewContractRegistry(q),
		Transactions:        mongoadapter.NewTransactionManager(db.Client(), db.Database()),
		NewEventTxRepositories: mongoadapter.NewEventTxRepositories,
		NewRuleTxRepositories:  mongoadapter.NewRuleTxRepositories,
	}, quarantineRepo.BackfillEffectTenantIDs
}
