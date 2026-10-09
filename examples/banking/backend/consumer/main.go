package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	transactioninitiated "examples/banking/generated/go/transaction_initiated"
	compliancecheck "examples/banking/generated/go/compliance_check"
	complianceresult "examples/banking/generated/go/compliance_result"
	transactionsettled "examples/banking/generated/go/transaction_settled"
)

type EventEnvelope struct {
	ID           string            `json:"id"`
	Type         string            `json:"type"`
	TenantID     string            `json:"tenant_id"`
	PartitionKey string            `json:"partition_key"`
	WorkflowID   string            `json:"workflow_id,omitempty"`
	OccurredAt   time.Time         `json:"occurred_at"`
	Data         json.RawMessage   `json:"data"`
	Headers      map[string]string `json:"headers,omitempty"`
}

func main() {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Failed to create JetStream context: %v", err)
	}

	consumer, err := js.CreateOrUpdateConsumer(ctx, "flowrule", jetstream.ConsumerConfig{
		Durable:       "banking-consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: "events.shard.456",
		MaxDeliver:    10,
		AckWait:       30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	fmt.Println("Banking consumer started. Waiting for events...")

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Shutting down consumer...")
			return
		default:
			batch, err := consumer.Fetch(10, jetstream.FetchMaxWait(5*time.Second))
			if err != nil {
				log.Printf("Failed to fetch messages: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			for msg := range batch.Messages() {
				if msg == nil {
					continue
				}

				var event EventEnvelope
				if err := json.Unmarshal(msg.Data(), &event); err != nil {
					log.Printf("Failed to unmarshal event: %v", err)
					msg.Nak()
					continue
				}

				fmt.Printf("\nReceived event:\n")
				fmt.Printf("  ID: %s\n", event.ID)
				fmt.Printf("  Type: %s\n", event.Type)
				fmt.Printf("  Tenant: %s\n", event.TenantID)
				fmt.Printf("  PartitionKey: %s\n", event.PartitionKey)

				switch event.Type {
				case "transaction.initiated":
					var txn transactioninitiated.Initiated
					if err := json.Unmarshal(event.Data, &txn); err != nil {
						log.Printf("Failed to unmarshal transaction.initiated: %v", err)
					} else {
						fmt.Printf("  Transaction ID: %s\n", txn.TransactionId)
						fmt.Printf("  Account: %s\n", txn.AccountId)
						fmt.Printf("  Amount: %.2f %s\n", txn.Amount/100, txn.Currency)
						fmt.Printf("  Type: %s\n", txn.TransactionType)
					}
				case "compliance.check":
					var check compliancecheck.Check
					if err := json.Unmarshal(event.Data, &check); err != nil {
						log.Printf("Failed to unmarshal compliance.check: %v", err)
					} else {
						fmt.Printf("  Check ID: %s\n", check.CheckId)
						fmt.Printf("  Transaction: %s\n", check.TransactionId)
					}
				case "compliance.result":
					var result complianceresult.Result
					if err := json.Unmarshal(event.Data, &result); err != nil {
						log.Printf("Failed to unmarshal compliance.result: %v", err)
					} else {
						fmt.Printf("  Check ID: %s\n", result.CheckId)
						fmt.Printf("  Passed: %v\n", result.Passed)
						fmt.Printf("  Risk Level: %s\n", result.RiskLevel)
					}
				case "transaction.settled":
					var settled transactionsettled.Settled
					if err := json.Unmarshal(event.Data, &settled); err != nil {
						log.Printf("Failed to unmarshal transaction.settled: %v", err)
					} else {
						fmt.Printf("  Transaction ID: %s\n", settled.TransactionId)
						fmt.Printf("  Settled At: %s\n", settled.SettledAt)
					}
				}

				fmt.Printf("  Raw Data: %s\n", string(event.Data))

				if err := msg.Ack(); err != nil {
					log.Printf("Failed to ack message: %v", err)
				} else {
					fmt.Println("Event acknowledged")
				}
			}
		}
	}
}