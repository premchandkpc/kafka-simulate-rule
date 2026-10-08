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

type Execution struct {
	ID           string    `json:"id"`
	EventID      string    `json:"event_id"`
	TenantID     string    `json:"tenant_id"`
	RuleSet      string    `json:"rule_set"`
	Revision     int64     `json:"revision"`
	DecisionHash string    `json:"decision_hash"`
	Status       string    `json:"status"`
	Error        string    `json:"error,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
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

	// Create consumer
	consumer, err := js.CreateOrUpdateConsumer(ctx, "flowrule", jetstream.ConsumerConfig{
		Durable:       "ecommerce-consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: "events.shard.123",
		MaxDeliver:    10,
		AckWait:       30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	fmt.Println("Ecommerce consumer started. Waiting for events...")

	// Consume messages
	msgs, err := consumer.Messages()
	if err != nil {
		log.Fatalf("Failed to create message iterator: %v", err)
	}
	defer msgs.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Shutting down consumer...")
			return
		case msg := <-msgs:
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
			fmt.Printf("  Data: %s\n", string(event.Data))

			// In a real app, you would call the FlowRule API to check execution status
			// For now, just ack the message
			if err := msg.Ack(); err != nil {
				log.Printf("Failed to ack message: %v", err)
			} else {
				fmt.Println("Event acknowledged")
			}
		}
	}
}