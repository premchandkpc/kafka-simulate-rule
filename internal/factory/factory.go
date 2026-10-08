package factory

import (
	"context"
	"time"

	"github.com/flowrule/flowrule/internal/adapters/mongo"
	"github.com/flowrule/flowrule/internal/adapters/nats"
	"github.com/flowrule/flowrule/internal/adapters/redis"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/flowrule/flowrule/internal/runtime/scheduler"
	"github.com/flowrule/flowrule/internal/runtime/shard"
	"github.com/flowrule/flowrule/internal/runtime/keyqueue"
	"github.com/flowrule/flowrule/internal/services/events"
	"github.com/flowrule/flowrule/internal/services/rules"
	"github.com/flowrule/flowrule/internal/services/effects"
	"github.com/flowrule/flowrule/internal/services/batches"
	"github.com/flowrule/flowrule/internal/application"
)

type Config struct {
	MongoDB    MongoConfig
	NATS       NATSConfig
	Redis      RedisConfig
	Worker     WorkerConfig
	Scheduler  SchedulerConfig
	KeyQueue   KeyQueueConfig
	ShardLease ShardLeaseConfig
	Batch      BatchConfig
}

type MongoConfig struct {
	URI        string
	Database   string
	MaxPool    uint64
	MinPool    uint64
	MaxConnIdle time.Duration
}

type NATSConfig struct {
	URL         string
	Stream      string
	Consumer    string
	Subjects    []string
	AckWait     time.Duration
	MaxDeliver  int
	NumShards   uint32
}

type RedisConfig struct {
	Addr         string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
	MaxRetries   int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type WorkerConfig struct {
	MaxGlobalInFlight int
	MaxPerKeyQueue    int
	KeyQueueWorkers   int
}

type SchedulerConfig struct {
	PollInterval time.Duration
	BatchSize    int
}

type KeyQueueConfig struct {
	MaxGlobalInFlight  int
	MaxPerKeyQueue     int
	WorkerCount        int
	HotKeyThreshold    int
	HotKeyCheckInterval time.Duration
}

type ShardLeaseConfig struct {
	NumShards      uint32
	LeaseTTL       time.Duration
	RenewalInterval time.Duration
}

type BatchConfig struct {
	Mode     domain.BatchMode
	MaxBatch int
	Window   time.Duration
}

type Factory struct {
	config Config
	mongoDB *mongo.DB
	redisClient *redis.Client
	natsConsumer *nats.Consumer
	natsPublisher *nats.Publisher
}

func NewFactory(config Config) *Factory {
	return &Factory{config: config}
}

func (f *Factory) MongoDB(ctx context.Context) (*mongo.DB, error) {
	if f.mongoDB != nil {
		return f.mongoDB, nil
	}
	
	db, err := mongo.New(ctx, mongo.Config{
		URI:        f.config.MongoDB.URI,
		Database:   f.config.MongoDB.Database,
		MaxPool:    f.config.MongoDB.MaxPool,
		MinPool:    f.config.MongoDB.MinPool,
		MaxConnIdle: f.config.MongoDB.MaxConnIdle,
	})
	if err != nil {
		return nil, err
	}
	f.mongoDB = db
	return db, nil
}

func (f *Factory) RedisClient(ctx context.Context) (*redis.Client, error) {
	if f.redisClient != nil {
		return f.redisClient, nil
	}
	
	client, err := redis.New(ctx, redis.Config{
		Addr:         f.config.Redis.Addr,
		Password:     f.config.Redis.Password,
		DB:           f.config.Redis.DB,
		PoolSize:     f.config.Redis.PoolSize,
		MinIdleConns: f.config.Redis.MinIdleConns,
		MaxRetries:   f.config.Redis.MaxRetries,
		DialTimeout:  f.config.Redis.DialTimeout,
		ReadTimeout:  f.config.Redis.ReadTimeout,
		WriteTimeout: f.config.Redis.WriteTimeout,
	})
	if err != nil {
		return nil, err
	}
	f.redisClient = client
	return client, nil
}

func (f *Factory) NATSConsumer(ctx context.Context) (*nats.Consumer, error) {
	if f.natsConsumer != nil {
		return f.natsConsumer, nil
	}
	
	consumer, err := nats.NewConsumer(ctx, nats.Config{
		NatsURL:    f.config.NATS.URL,
		Stream:     f.config.NATS.Stream,
		Consumer:   f.config.NATS.Consumer,
		Subjects:   f.config.NATS.Subjects,
		AckWait:    f.config.NATS.AckWait,
		MaxDeliver: f.config.NATS.MaxDeliver,
		NumShards:  f.config.NATS.NumShards,
	})
	if err != nil {
		return nil, err
	}
	f.natsConsumer = consumer
	return consumer, nil
}

func (f *Factory) NATSPublisher(ctx context.Context) (*nats.Publisher, error) {
	if f.natsPublisher != nil {
		return f.natsPublisher, nil
	}
	
	publisher, err := nats.NewPublisher(nats.Config{
		NatsURL:  f.config.NATS.URL,
		Stream:   f.config.NATS.Stream,
		NumShards: f.config.NATS.NumShards,
	})
	if err != nil {
		return nil, err
	}
	f.natsPublisher = publisher
	return publisher, nil
}

func (f *Factory) TransactionManager(db *mongo.DB) ports.TransactionManager {
	return mongo.NewTransactionManager(db.Client())
}

func (f *Factory) EventRepository(db *mongo.DB) *mongo.EventRepository {
	return mongo.NewEventRepository(mongo.NewQuerier(db.Database()))
}

func (f *Factory) InboxRepository(db *mongo.DB) *mongo.InboxRepository {
	return mongo.NewInboxRepository(mongo.NewQuerier(db.Database()))
}

func (f *Factory) ExecutionRepository(db *mongo.DB) *mongo.ExecutionRepository {
	return mongo.NewExecutionRepository(mongo.NewQuerier(db.Database()))
}

func (f *Factory) OutboxRepository(db *mongo.DB) *mongo.OutboxRepository {
	return mongo.NewOutboxRepository(mongo.NewQuerier(db.Database()))
}

func (f *Factory) RuleRepository(db *mongo.DB) *mongo.Repository {
	return mongo.NewRepository(mongo.NewQuerier(db.Database()))
}

func (f *Factory) ActivationRepository(db *mongo.DB) *mongo.ActivationRepository {
	return mongo.NewActivationRepository(mongo.NewQuerier(db.Database()))
}

func (f *Factory) WorkflowRepository(db *mongo.DB) *mongo.WorkflowRepository {
	return mongo.NewWorkflowRepository(mongo.NewQuerier(db.Database()))
}

func (f *Factory) ScheduledEventRepository(db *mongo.DB) *mongo.ScheduledEventRepository {
	return mongo.NewScheduledEventRepository(mongo.NewQuerier(db.Database()))
}

func (f *Factory) BatchRepository(db *mongo.DB) *mongo.BatchRepository {
	return mongo.NewBatchRepository(mongo.NewQuerier(db.Database()))
}

func (f *Factory) ShardLeaseRepository(db *mongo.DB) *mongo.ShardLeaseRepository {
	return mongo.NewShardLeaseRepository(mongo.NewQuerier(db.Database()))
}

func (f *Factory) QuarantineRepository(db *mongo.DB) *mongo.QuarantineRepository {
	return mongo.NewQuarantineRepository(mongo.NewQuerier(db.Database()))
}

func (f *Factory) ContractRegistry(db *mongo.DB) *mongo.ContractRegistry {
	return mongo.NewContractRegistry(mongo.NewQuerier(db.Database()))
}

func (f *Factory) ShardManager(db *mongo.DB, clock ports.Clock, workerID string) *shard.LeaseManager {
	return shard.NewLeaseManager(
		mongo.NewShardLeaseRepository(mongo.NewQuerier(db.Database())),
		clock,
		workerID,
		f.config.ShardLease.NumShards,
		f.config.ShardLease.LeaseTTL,
	)
}

func (f *Factory) KeyQueue(processor func(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error, clock ports.Clock) *keyqueue.KeyQueue {
	return keyqueue.NewKeyQueue(
		f.config.KeyQueue.MaxGlobalInFlight,
		f.config.KeyQueue.MaxPerKeyQueue,
		f.config.KeyQueue.WorkerCount,
		processor,
		clock.Now,
		f.config.KeyQueue.HotKeyThreshold,
		f.config.KeyQueue.HotKeyCheckInterval,
		nil, // hotKeyCallback - can be set separately
	)
}

func (f *Factory) Scheduler(db *mongo.DB, publisher ports.BrokerPublisher, clock ports.Clock) *scheduler.Scheduler {
	return scheduler.NewScheduler(
		mongo.NewScheduledEventRepository(mongo.NewQuerier(db.Database())),
		publisher,
		clock,
		f.config.Scheduler.PollInterval,
		f.config.Scheduler.BatchSize,
	)
}

func (f *Factory) EventService(
	compiler ports.RuleCompiler,
	evaluator ports.RuleEvaluator,
	quarantine *mongo.QuarantineRepository,
	clock ports.Clock,
	beginTx ports.TransactionManager,
	newRepos func(ports.Transaction) ports.TxRepos,
) *events.Service {
	return events.NewService(compiler, evaluator, quarantine, clock, beginTx, newRepos)
}

func (f *Factory) RuleService(
	compiler ports.RuleCompiler,
	ruleRepo *mongo.Repository,
	activation *mongo.ActivationRepository,
	executions *mongo.ExecutionRepository,
	clock ports.Clock,
) *rules.Service {
	return rules.NewService(compiler, ruleRepo, activation, executions, clock)
}

func (f *Factory) EffectService(
	outbox *mongo.OutboxRepository,
	sender ports.EffectSender,
	quarantine *mongo.QuarantineRepository,
	clock ports.Clock,
) *effects.Service {
	return effects.NewService(outbox, sender, quarantine, clock)
}

func (f *Factory) BatchService(
	batchRepo *mongo.BatchRepository,
	events ports.EventProcessor,
	clock ports.Clock,
	cfg domain.BatchConfig,
) *batches.Service {
	return batches.NewService(batchRepo, events, clock, cfg)
}

func (f *Factory) Worker(
	consumer ports.ShardedConsumer,
	events ports.EventProcessor,
	effects ports.EffectPublisher,
	batches *batches.Service,
	batchInterval time.Duration,
	clock ports.Clock,
	leaseManager *shard.LeaseManager,
	workerID string,
	numShards uint32,
	cfg application.WorkerConfig,
) *application.Worker {
	return application.NewWorker(
		consumer, events, effects, batches, batchInterval, clock,
		leaseManager, workerID, numShards, cfg,
	)
}

func (f *Factory) Close() error {
	var errs []error
	if f.mongoDB != nil {
		f.mongoDB.Close()
	}
	if f.redisClient != nil {
		if err := f.redisClient.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if f.natsConsumer != nil {
		f.natsConsumer.Close()
	}
	if f.natsPublisher != nil {
		f.natsPublisher.Close()
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}