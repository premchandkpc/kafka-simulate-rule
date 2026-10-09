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

	orderplaced "examples/food-delivery/generated/go/order_placed"
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

	// Create order using generated types
	order := orderplaced.Placed{
		OrderId:      "order-001",
		CustomerId:   "customer-789",
		RestaurantId: "restaurant-001",
		TotalAmount:  2800,
		Currency:     "USD",
		Items: []orderplaced.Item{
			{MenuItemId: "pizza-margherita", Quantity: 1, UnitPrice: 1800, Name: "Pizza Margherita"},
			{MenuItemId: "garlic-bread", Quantity: 2, UnitPrice: 500, Name: "Garlic Bread"},
		},
		DeliveryAddress: orderplaced.DeliveryAddress{
			Street:      "456 Oak Ave",
			City:        "San Francisco",
			State:       "CA",
			PostalCode:  "94102",
		},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	orderData, err := json.Marshal(order)
	if err != nil {
		log.Fatalf("Failed to marshal order: %v", err)
	}

	orderEvent := EventEnvelope{
		ID:           "order-001",
		Type:         "order.placed",
		TenantID:     "food-delivery-platform",
		PartitionKey: "94102",
		OccurredAt:   time.Now().UTC(),
		Data:         orderData,
	}

	data, err := json.Marshal(orderEvent)
	if err != nil {
		log.Fatalf("Failed to marshal event: %v", err)
	}

	shard := uint32(94102)
	subject := fmt.Sprintf("events.shard.%d", shard)

	_, err = js.Publish(ctx, subject, data)
	if err != nil {
		log.Fatalf("Failed to publish event: %v", err)
	}

	fmt.Println("Order placed event published successfully!")
	fmt.Printf("Event ID: %s\n", orderEvent.ID)
	fmt.Printf("Type: %s\n", orderEvent.Type)
	fmt.Printf("Tenant: %s\n", orderEvent.TenantID)
}