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

	// Create stream if not exists
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "flowrule",
		Subjects: []string{"events.>"},
	})
	if err != nil {
		log.Fatalf("Failed to create stream: %v", err)
	}

	// Example: Create an order
	orderEvent := EventEnvelope{
		ID:           "order-001",
		Type:         "order.created",
		TenantID:     "acme-corp",
		PartitionKey: "customer-123",
		OccurredAt:   time.Now().UTC(),
		Data: json.RawMessage(`{
			"order_id": "order-001",
			"customer_id": "customer-123",
			"total_amount": 25000,
			"currency": "USD",
			"items": [
				{"product_id": "prod-001", "quantity": 2, "unit_price": 10000},
				{"product_id": "prod-002", "quantity": 1, "unit_price": 5000}
			],
			"shipping_address": {
				"street": "123 Main St",
				"city": "San Francisco",
				"state": "CA",
				"postal_code": "94102",
				"country": "US"
			},
			"created_at": "` + time.Now().UTC().Format(time.RFC3339) + `"
		}`),
	}

	data, err := json.Marshal(orderEvent)
	if err != nil {
		log.Fatalf("Failed to marshal event: %v", err)
	}

	// Publish to shard
	shard := uint32(123) // In real app, compute from tenant_id + partition_key
	subject := fmt.Sprintf("events.shard.%d", shard)

	_, err = js.Publish(ctx, subject, data)
	if err != nil {
		log.Fatalf("Failed to publish event: %v", err)
	}

	fmt.Println("Order created event published successfully!")
	fmt.Printf("Event ID: %s\n", orderEvent.ID)
	fmt.Printf("Type: %s\n", orderEvent.Type)
	fmt.Printf("Tenant: %s\n", orderEvent.TenantID)
}
