//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/flowrule/flowrule/internal/adapters/nats"
	"github.com/flowrule/flowrule/internal/application"
	"github.com/flowrule/flowrule/internal/config"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/rules"
	"github.com/flowrule/flowrule/internal/runtime/scheduler"
	"github.com/flowrule/flowrule/internal/runtime/shard"
	"github.com/flowrule/flowrule/internal/services/batches"
	svceffects "github.com/flowrule/flowrule/internal/services/effects"
	svcevents "github.com/flowrule/flowrule/internal/services/events"
	"github.com/flowrule/flowrule/internal/storage"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	natsgo "github.com/nats-io/nats.go"
	natstest "github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	testcontainers "github.com/testcontainers/testcontainers-go"
)

type TestEnv struct {
	PostgresContainer *postgres.PostgresContainer
	NATSContainer     *natstest.NATSContainer
	PostgresPool      *pgxpool.Pool
	NATSConn          *natsgo.Conn
	WorkerCancel      context.CancelFunc
	WorkerDone        chan error
	Config            *config.Config
	Storage           *storage.Storage
	ShardCount        uint32
}

type fakeEffectSender struct{}

func (f *fakeEffectSender) Send(ctx context.Context, effect *domain.Effect, idempotencyKey string) error {
	log.Printf("fake sender: effect %s to %s", effect.ID, effect.Destination)
	return nil
}

func StartStack(t *testing.T) *TestEnv {
	ctx := context.Background()

	// Start PostgreSQL
	pgContainer, err := postgres.RunContainer(ctx,
		postgres.WithDatabase("flowrule"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}

	// Start NATS with JetStream
	natsContainer, err := natstest.RunContainer(ctx,
		testcontainers.WithWaitStrategy(
			wait.ForLog("Server is ready").
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		pgContainer.Terminate(ctx)
		t.Fatalf("start nats: %v", err)
	}

	// Get connection details
	pgDSN, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}

	natsURL, err := natsContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("nats url: %v", err)
	}

	// Connect to NATS
	natsConn, err := natsgo.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}

	// Create config
	cfg := &config.Config{
		App: config.AppConfig{
			Env:             "test",
			LogLevel:        "debug",
			HTTPAddr:        "127.0.0.1:0",
			ShutdownTimeout: 10 * time.Second,
		},
		Database: config.DatabaseConfig{
			StorageBackend: "postgres",
			DSN:            pgDSN,
			MigrationsDir:  "../../migrations",
		},
		NATS: config.NATSConfig{
			URL:      natsURL,
			Stream:   "flowrule",
			Consumer: "e2e-test",
		},
		Worker: config.WorkerConfig{
			NumShards:               16,
			WorkerReplicas:          1,
			MaxGlobalInFlight:       100,
			FetchBatchSize:          50,
			EffectPublishIntervalMS: 1000,
			EffectPublishBatchSize:  10,
			RetryDelayMS:            500,
			MaxPerKeyQueue:          100,
			KeyQueueWorkers:         1,
			BatchMode:               "none",
			BatchMax:                10,
			BatchWindowMS:           5000,
			SchedulerPollMS:         1000,
			SchedulerBatchSize:      50,
			LeaseTTLMS:              10000,
			LeaseRenewalIntervalMS:  3000,
			NATSAckWaitMS:           5000,
			NATSMaxDeliver:          -1,
			HotKeyThreshold:         100,
			HotKeyCheckIntervalMS:   5000,
		},
	}

	// Open storage (runs migrations)
	storage, err := storage.Open(ctx, cfg.Database)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	if err := storage.Initialize(ctx); err != nil {
		t.Fatalf("init storage: %v", err)
	}

	// Start worker
	repositories := storage.Repositories()
	compiler := rules.NewCompiler(rules.DefaultLimits())
	clock := domain.SystemClock{}
	evaluator := rules.NewEvaluator(clock)

	// Use fake effect sender for testing
	effectSender := &fakeEffectSender{}

	eventsSvc := svcevents.NewService(
		compiler, evaluator, repositories.Quarantine, clock,
		repositories.Transactions, repositories.NewEventTxRepositories,
	)
	effectsSvc := svceffects.NewService(
		repositories.Outbox, effectSender, repositories.Quarantine, clock, "e2e-worker",
	)

	batchSvc := batches.NewService(
		repositories.Batches, eventsSvc, clock,
		domain.BatchConfig{Mode: domain.BatchModeNone},
	)

	// Create NATS consumer for worker
	natsCfg := nats.Config{
		NatsURL:    cfg.NATS.URL,
		Stream:     cfg.NATS.Stream,
		Consumer:   cfg.NATS.Consumer,
		Subjects:   []string{"events"},
		AckWait:    cfg.NATSAckWait(),
		MaxDeliver: cfg.Worker.NATSMaxDeliver,
		NumShards:  cfg.Worker.NumShards,
	}
	consumer, err := nats.NewConsumer(ctx, natsCfg)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}

	workerID := "e2e-worker-" + uuid.New().String()[:8]
	workerConfig := application.DefaultWorkerConfig()
	workerConfig.MaxGlobalInFlight = cfg.Worker.MaxGlobalInFlight
	workerConfig.FetchBatchSize = cfg.Worker.FetchBatchSize
	workerConfig.EffectPublishInterval = cfg.EffectPublishInterval()
	workerConfig.EffectPublishBatchSize = cfg.Worker.EffectPublishBatchSize
	workerConfig.RetryDelay = cfg.EventRetryDelay()
	workerConfig.MaxPerKeyQueue = cfg.Worker.MaxPerKeyQueue
	workerConfig.KeyQueueWorkers = cfg.Worker.KeyQueueWorkers
	workerConfig.HotKeyThreshold = cfg.Worker.HotKeyThreshold
	workerConfig.HotKeyCheckInterval = cfg.HotKeyCheckInterval()

	leaseManager := shard.NewLeaseManagerWithReplicaCountAndRenewInterval(
		repositories.ShardLeases,
		clock,
		workerID,
		cfg.Worker.NumShards,
		cfg.Worker.WorkerReplicas,
		cfg.LeaseTTL(),
		cfg.LeaseRenewalInterval(),
	)

	worker, err := application.NewWorker(
		consumer,
		eventsSvc,
		effectsSvc,
		batchSvc,
		cfg.SchedulerPollInterval(),
		clock,
		leaseManager,
		workerID,
		cfg.Worker.NumShards,
		workerConfig,
	)
	if err != nil {
		t.Fatalf("construct worker: %v", err)
	}

	// Start scheduler
	sched := scheduler.NewScheduler(
		repositories.ScheduledEvents,
		consumer,
		clock,
		cfg.SchedulerPollInterval(),
		cfg.Worker.SchedulerBatchSize,
		cfg.Worker.NumShards,
	)

	workerCtx, workerCancel := context.WithCancel(ctx)
	sched.Start(workerCtx)

	workerDone := make(chan error, 1)
	go func() {
		if err := worker.Run(workerCtx); err != nil {
			log.Printf("worker error: %v", err)
		}
		close(workerDone)
	}()

	return &TestEnv{
		PostgresContainer: pgContainer,
		NATSContainer:     natsContainer,
		NATSConn:          natsConn,
		Storage:           storage,
		Config:            cfg,
		ShardCount:        cfg.Worker.NumShards,
		WorkerCancel:      workerCancel,
		WorkerDone:        workerDone,
	}
}

func (e *TestEnv) Stop() {
	if e.WorkerCancel != nil {
		e.WorkerCancel()
		select {
		case <-e.WorkerDone:
		case <-time.After(5 * time.Second):
			log.Println("worker shutdown timeout")
		}
	}
	if e.Storage != nil {
		e.Storage.Close()
	}
	if e.NATSConn != nil {
		e.NATSConn.Close()
	}
	if e.NATSContainer != nil {
		e.NATSContainer.Terminate(context.Background())
	}
	if e.PostgresContainer != nil {
		e.PostgresContainer.Terminate(context.Background())
	}
}

func (e *TestEnv) Publish(t *testing.T, envelope *domain.EventEnvelope) {
	data, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	shard := envelope.VirtualShard(e.ShardCount)
	subject := fmt.Sprintf("events.shard.%d", shard)

	if err := e.NATSConn.Publish(subject, data); err != nil {
		t.Fatalf("publish event: %v", err)
	}
	if err := e.NATSConn.Flush(); err != nil {
		t.Fatalf("flush nats: %v", err)
	}
}

func (e *TestEnv) ExecutionCount(t *testing.T, tenantID, eventID string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exec, err := e.Storage.Repositories().Executions.Get(ctx, domain.ComputeExecutionID(tenantID, eventID, "order.created", 1))
	if err != nil {
		return 0
	}
	if exec == nil {
		return 0
	}
	return 1
}

func (e *TestEnv) WaitForExecution(t *testing.T, tenantID, eventID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if e.ExecutionCount(t, tenantID, eventID) > 0 {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func (e *TestEnv) DeployRule(t *testing.T, tenantScope, ruleSet string, ruleSource json.RawMessage) {
	ctx := context.Background()
	repositories := e.Storage.Repositories()
	compiler := rules.NewCompiler(rules.DefaultLimits())

	revision, err := compiler.Compile(ruleSource)
	if err != nil {
		t.Fatalf("compile rule: %v", err)
	}
	revision.TenantScope = tenantScope
	revision.RuleID = ruleSet
	if err := repositories.Rules.Save(ctx, revision); err != nil {
		t.Fatalf("save revision: %v", err)
	}

	activation := &domain.RuleActivation{
		TenantScope: tenantScope,
		RuleSet:     ruleSet,
		Revision:    1,
		Version:     1,
		Actor:       "e2e-test",
		ActivatedAt: domain.SystemClock{}.Now(),
	}
	if err := repositories.Activations.Set(ctx, activation); err != nil {
		t.Fatalf("set activation: %v", err)
	}
}
