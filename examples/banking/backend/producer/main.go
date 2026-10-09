package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

	ctx := context.Background()

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Failed to create JetStream context: %v", err)
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "flowrule",
		Subjects: []string{"events.>"},
	})
	if err != nil {
		log.Fatalf("Failed to create stream: %v", err)
	}

	transactionEvent := EventEnvelope{
		ID:           "txn-001",
		Type:         "transaction.initiated",
		TenantID:     "global-bank",
		PartitionKey: "account-456",
		OccurredAt:   time.Now().UTC(),
		Data: json.RawMessage(`{
			"transaction_id": "txn-001",
			"account_id": "account-456",
			"amount": 2500000,
			"currency": "USD",
			"type": "wire_transfer",
			"counterparty": {
				"name": "International Corp",
				"account": "INTL-789",
				"country": "GB"
			},
			"reference": "INV-2024-001",
			"initiated_at": "` + time.Now().UTC().Format(time.RFC3339) + `"
		}`),
	}

	data, err := json.Marshal(transactionEvent)
	if err != nil {
		log.Fatalf("Failed to marshal event: %v", err)
	}

	shard := uint32(456)
	subject := fmt.Sprintf("events.shard.%d", shard)

	_, err = js.Publish(ctx, subject, data)
	if err != nil {
		log.Fatalf("Failed to publish event: %v", err)
	}

	fmt.Println("Transaction initiated event published successfully!")
	fmt.Printf("Event ID: %s\n", transactionEvent.ID)
	fmt.Printf("Type: %s\n", transactionEvent.Type)
	fmt.Printf("Tenant: %s\n", transactionEvent.TenantID)
}