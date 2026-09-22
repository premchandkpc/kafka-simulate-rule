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
	clock := ports.SystemClock{}
	effectSender := effects.NewFakeDestination()

	pool := db.Pool()

	beginTx := func(ctx context.Context) (ports.Tx, error) {
		return pool.Begin(ctx)
	}

	newRepos := func(q ports.Querier) application.TxRepos {
		return application.TxRepos{
			Inbox:       sql.NewInboxRepository(q),
			Activations: sql.NewActivationRepository(q),
			RuleRepo:    sql.NewRuleRepository(q),
			Executions:  sql.NewExecutionRepository(q),
			Outbox:      sql.NewOutboxRepository(q),
		}
	}

	processUC := application.NewProcessEventUseCase(compiler, evaluator, effectSender, clock, beginTx, newRepos)

	outboxRepo := sql.NewOutboxRepository(pool)
	quarantineRepo := sql.NewQuarantineRepository(pool)
	publishUC := application.NewPublishEffectsUseCase(outboxRepo, effectSender, nil, quarantineRepo, clock)

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
				if err := quarantineRepo.Save(ctx, &domain.QuarantineEntry{
					ID:         domain.NewID(),
					SourceType: "event",
					SourceID:   domain.ComputeSourceHash(delivery.Raw()),
					ErrorClass: string(domain.ErrorClassValidation),
					PayloadRef: "jetstream:flowrule",
					Error:      "invalid event envelope JSON",
					CreatedAt:  clock.Now(),
				}); err != nil {
					log.Printf("quarantine invalid event: %v", err)
					delivery.Retry(ctx, 5*time.Second)
					continue
				}
				delivery.Ack(ctx)
				continue
			}

			exec, err := processUC.Execute(ctx, env)
			if err != nil {
				log.Printf("process event %s: %v", env.ID, err)
				if domain.IsPermanent(err) {
					if qErr := quarantineRepo.Save(ctx, &domain.QuarantineEntry{
						ID:         domain.NewID(),
						SourceType: "event",
						SourceID:   env.ID,
						EventID:    env.ID,
						TenantID:   env.TenantID,
						ErrorClass: string(domain.ClassifyError(err)),
						PayloadRef: "jetstream:flowrule",
						Error:      err.Error(),
						CreatedAt:  clock.Now(),
					}); qErr != nil {
						log.Printf("quarantine event %s: %v", env.ID, qErr)
						delivery.Retry(ctx, 5*time.Second)
						continue
					}
					delivery.Ack(ctx)
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
