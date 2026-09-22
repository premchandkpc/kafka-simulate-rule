package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/flowrule/flowrule/internal/adapters/effects"
	"github.com/flowrule/flowrule/internal/adapters/nats"
	"github.com/flowrule/flowrule/internal/adapters/sql"
	"github.com/flowrule/flowrule/internal/application"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/flowrule/flowrule/internal/rules"
	svceffects "github.com/flowrule/flowrule/internal/services/effects"
	svcevents "github.com/flowrule/flowrule/internal/services/events"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/flowrule?sslmode=disable"
	}
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
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

	consumer, err := nats.NewConsumer(ctx, nats.Config{
		NatsURL:    natsURL,
		Stream:     "flowrule",
		Consumer:   "flowrule-worker",
		Subjects:   []string{"events.>"},
		AckWait:    30 * time.Second,
		MaxDeliver: 10,
	})
	if err != nil {
		log.Fatalf("nats consumer: %v", err)
	}
	defer consumer.Close()

	compiler := rules.NewCompiler(rules.DefaultLimits())
	evaluator := rules.NewEvaluator()
	clock := domain.SystemClock{}
	effectSender := effects.NewFakeDestination()
	pool := db.Pool()
	quarantineRepo := sql.NewQuarantineRepository(pool)

	beginTx := func(ctx context.Context) (ports.Tx, error) {
		pgxTx, err := pool.Begin(ctx)
		if err != nil {
			return nil, err
		}
		return sql.NewTx(pgxTx), nil
	}
	newRepos := func(db interface{}) svcevents.TxRepos {
		q := db.(sql.Querier)
		return svcevents.TxRepos{
			Inbox:       sql.NewInboxRepository(q),
			Activations: sql.NewActivationRepository(q),
			RuleRepo:    sql.NewRuleRepository(q),
			Executions:  sql.NewExecutionRepository(q),
			Outbox:      sql.NewOutboxRepository(q),
		}
	}

	eventsSvc := svcevents.NewService(compiler, evaluator, quarantineRepo, clock, beginTx, newRepos)
	outboxRepo := sql.NewOutboxRepository(pool)
	effectsSvc := svceffects.NewService(outboxRepo, effectSender, quarantineRepo, clock)

	worker := application.NewWorker(consumer, eventsSvc, effectsSvc, clock)
	log.Println("worker started, fetching events...")
	worker.Run(ctx)
}
