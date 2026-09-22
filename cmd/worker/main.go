package main

import (
	"context"
	"encoding/json"
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
		NatsURL:  natsURL,
		Stream:   "flowrule",
		Consumer: "flowrule-worker",
		Subjects: []string{"events.>"},
		AckWait:  30 * time.Second,
	})
	if err != nil {
		log.Fatalf("nats consumer: %v", err)
	}
	defer consumer.Close()

	compiler := rules.NewCompiler(rules.DefaultLimits())
	evaluator := rules.NewEvaluator()
	clock := ports.SystemClock{}

	inboxRepo := sql.NewInboxRepository(db.Pool())
	activationRepo := sql.NewActivationRepository(db.Pool())
	ruleRepo := sql.NewRuleRepository(db.Pool())
	executionRepo := sql.NewExecutionRepository(db.Pool())
	outboxRepo := sql.NewOutboxRepository(db.Pool())
	quarantineRepo := &fakeQuarantineRepo{entries: make(map[string]*domain.QuarantineEntry)}
	effectSender := effects.NewFakeDestination()

	processUC := application.NewProcessEventUseCase(compiler, evaluator, inboxRepo, activationRepo, ruleRepo, executionRepo, outboxRepo, effectSender, clock)
	publishUC := application.NewPublishEffectsUseCase(outboxRepo, effectSender, executionRepo, quarantineRepo, clock)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := publishUC.Execute(ctx, 10); err != nil {
					log.Printf("publish effects: %v", err)
				}
			}
		}
	}()

	log.Println("worker started, fetching events...")
	for {
		select {
		case <-ctx.Done():
			log.Println("worker shutting down...")
			return
		default:
		}

		deliveries, err := consumer.Fetch(ctx, 10)
		if err != nil {
			log.Printf("fetch: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, delivery := range deliveries {
			env := delivery.Event()
			if env == nil {
				data, _ := json.Marshal(delivery.Raw())
				log.Printf("invalid event: %s", string(data))
				delivery.Nak(ctx)
				continue
			}

			exec, err := processUC.Execute(ctx, env)
			if err != nil {
				log.Printf("process event %s: %v", env.ID, err)
				if domain.IsPermanent(err) {
					delivery.Nak(ctx)
				} else {
					delivery.Retry(ctx, 5*time.Second)
				}
				continue
			}

			if err := delivery.Ack(ctx); err != nil {
				log.Printf("ack %s: %v", env.ID, err)
			} else {
				log.Printf("processed event %s -> execution %s", env.ID, exec.ID)
			}
		}
	}
}

type fakeQuarantineRepo struct {
	entries map[string]*domain.QuarantineEntry
}

func (f *fakeQuarantineRepo) Save(ctx context.Context, entry *domain.QuarantineEntry) error {
	f.entries[entry.ID] = entry
	return nil
}
func (f *fakeQuarantineRepo) Get(ctx context.Context, id string) (*domain.QuarantineEntry, error) {
	return f.entries[id], nil
}
func (f *fakeQuarantineRepo) Replay(ctx context.Context, id string) error {
	return nil
}