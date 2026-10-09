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

	ordercreated "examples/ecommerce/generated/go/order_created"
	paymentrequested "examples/ecommerce/generated/go/payment_requested"
	paymentcompleted "examples/ecommerce/generated/go/payment_completed"
	fraudresult "examples/ecommerce/generated/go/fraud_result"
	fraudreview "examples/ecommerce/generated/go/fraud_review"
	orderconfirmed "examples/ecommerce/generated/go/order_confirmed"
	shipmentrequested "examples/ecommerce/generated/go/shipment_requested"
	fraudcheck "examples/ecommerce/generated/go/fraud_check"
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

	// Consume messages using Fetch
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

				// Unmarshal data using generated types based on event type
				switch event.Type {
				case "order.created":
					var order ordercreated.Created
					if err := json.Unmarshal(event.Data, &order); err != nil {
						log.Printf("Failed to unmarshal order.created: %v", err)
					} else {
						fmt.Printf("  Order ID: %s\n", order.OrderId)
						fmt.Printf("  Customer: %s\n", order.CustomerId)
						fmt.Printf("  Total: %.2f %s\n", order.TotalAmount/100, order.Currency)
					}
				case "payment.requested":
					var payment paymentrequested.Requested
					if err := json.Unmarshal(event.Data, &payment); err != nil {
						log.Printf("Failed to unmarshal payment.requested: %v", err)
					} else {
						fmt.Printf("  Payment ID: %s\n", payment.PaymentId)
					}
				case "payment.completed":
					var payment paymentcompleted.Completed
					if err := json.Unmarshal(event.Data, &payment); err != nil {
						log.Printf("Failed to unmarshal payment.completed: %v", err)
					} else {
						fmt.Printf("  Payment ID: %s\n", payment.PaymentId)
					}
				case "fraud.result":
					var fraud fraudresult.Result
					if err := json.Unmarshal(event.Data, &fraud); err != nil {
						log.Printf("Failed to unmarshal fraud.result: %v", err)
					} else {
						fmt.Printf("  Fraud Check ID: %s\n", fraud.CheckId)
						fmt.Printf("  Passed: %v\n", fraud.Passed)
						fmt.Printf("  Risk Score: %.2f\n", fraud.RiskScore)
					}
				case "fraud.review":
					var fraud fraudreview.Review
					if err := json.Unmarshal(event.Data, &fraud); err != nil {
						log.Printf("Failed to unmarshal fraud.review: %v", err)
					} else {
						fmt.Printf("  Fraud Check ID: %s\n", fraud.CheckId)
						fmt.Printf("  Risk Score: %.2f\n", fraud.RiskScore)
					}
				case "order.confirmed":
					var order orderconfirmed.Confirmed
					if err := json.Unmarshal(event.Data, &order); err != nil {
						log.Printf("Failed to unmarshal order.confirmed: %v", err)
					} else {
						fmt.Printf("  Order ID: %s\n", order.OrderId)
					}
				case "shipment.requested":
					var shipment shipmentrequested.Requested
					if err := json.Unmarshal(event.Data, &shipment); err != nil {
						log.Printf("Failed to unmarshal shipment.requested: %v", err)
					} else {
						fmt.Printf("  Shipment ID: %s\n", shipment.ShipmentId)
					}
				case "fraud.check":
					var fraud fraudcheck.Check
					if err := json.Unmarshal(event.Data, &fraud); err != nil {
						log.Printf("Failed to unmarshal fraud.check: %v", err)
					} else {
						fmt.Printf("  Fraud Check ID: %s\n", fraud.CheckId)
						fmt.Printf("  Amount: %.2f %s\n", fraud.Amount/100, fraud.Currency)
					}
				}

				fmt.Printf("  Raw Data: %s\n", string(event.Data))

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
}