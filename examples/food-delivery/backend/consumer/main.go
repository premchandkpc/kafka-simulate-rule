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

	orderplaced "examples/food-delivery/generated/go/order_placed"
	restaurantaccepted "examples/food-delivery/generated/go/restaurant_accepted"
	foodprepared "examples/food-delivery/generated/go/food_prepared"
	driverassigned "examples/food-delivery/generated/go/driver_assigned"
	delivered "examples/food-delivery/generated/go/delivered"
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
		Durable:       "food-delivery-consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		FilterSubject: "events.shard.94102",
		MaxDeliver:    10,
		AckWait:       30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	fmt.Println("Food Delivery consumer started. Waiting for events...")

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
				case "order.placed":
					var order orderplaced.Placed
					if err := json.Unmarshal(event.Data, &order); err != nil {
						log.Printf("Failed to unmarshal order.placed: %v", err)
					} else {
						fmt.Printf("  Order ID: %s\n", order.OrderId)
						fmt.Printf("  Customer: %s\n", order.CustomerId)
						fmt.Printf("  Restaurant: %s\n", order.RestaurantId)
						fmt.Printf("  Total: %.2f %s\n", order.TotalAmount/100, order.Currency)
					}
				case "restaurant.accepted":
					var accepted restaurantaccepted.Accepted
					if err := json.Unmarshal(event.Data, &accepted); err != nil {
						log.Printf("Failed to unmarshal restaurant.accepted: %v", err)
					} else {
						fmt.Printf("  Order ID: %s\n", accepted.OrderId)
						fmt.Printf("  Restaurant: %s\n", accepted.RestaurantId)
						fmt.Printf("  Est. Prep Time: %d min\n", accepted.EstimatedPrepTime)
					}
				case "food.prepared":
					var prepared foodprepared.Prepared
					if err := json.Unmarshal(event.Data, &prepared); err != nil {
						log.Printf("Failed to unmarshal food.prepared: %v", err)
					} else {
						fmt.Printf("  Order ID: %s\n", prepared.OrderId)
						fmt.Printf("  Restaurant: %s\n", prepared.RestaurantId)
					}
				case "driver.assigned":
					var assigned driverassigned.Assigned
					if err := json.Unmarshal(event.Data, &assigned); err != nil {
						log.Printf("Failed to unmarshal driver.assigned: %v", err)
					} else {
						fmt.Printf("  Order ID: %s\n", assigned.OrderId)
						fmt.Printf("  Driver: %s\n", assigned.DriverId)
					}
				case "delivered":
					var del delivered.Delivered
					if err := json.Unmarshal(event.Data, &del); err != nil {
						log.Printf("Failed to unmarshal delivered: %v", err)
					} else {
						fmt.Printf("  Order ID: %s\n", del.OrderId)
						fmt.Printf("  Driver: %s\n", del.DriverId)
						fmt.Printf("  Customer: %s\n", del.CustomerId)
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