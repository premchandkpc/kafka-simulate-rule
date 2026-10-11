// Example FlowRule client usage
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	flowrule "github.com/flowrule/flowrule-sdk/go/flowrule"
)

// OrderCreated represents an order created event
type OrderCreated struct {
	OrderID     string  `json:"order_id" partition_key:"true"`
	CustomerID  string  `json:"customer_id"`
	Total       float64 `json:"total"`
	Currency    string  `json:"currency"`
	Items       []OrderItem `json:"items"`
	CreatedAt   time.Time `json:"created_at"`
}

// OrderItem represents an item in an order
type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

// PaymentRequested represents a payment requested event (child event)
type PaymentRequested struct {
	PaymentID   string  `json:"payment_id" partition_key:"true"`
	OrderID     string  `json:"order_id"`
	Amount      float64 `json:"amount"`
	Currency    string  `json:"currency"`
	Method      string  `json:"method"`
}

func main() {
	// Option 1: Auto-config from environment variables
	// export FLOWRULE_TRANSPORT=nats
	// export FLOWRULE_TENANT_ID=acme
	// export NATS_URL=nats://localhost:4222
	// export NATS_STREAM=flowrule
	//
	// fs, err := flowrule.New()

	// Option 2: Explicit config (for testing or multiple clients)
	cfg := &flowrule.Config{
		Transport:    flowrule.TransportNATS,
		TenantID:     "acme",
		NATSURL:      "nats://localhost:4222",
		NATSStream:   "flowrule",
		NumShards:    4096,
		SendTimeout:  5 * time.Second,
		MaxRetries:   3,
		RetryBackoff: 100 * time.Millisecond,
	}

	fs, err := flowrule.NewWithConfig(cfg)
	if err != nil {
		log.Fatalf("create client: %v", err)
	}
	defer fs.Close()

	ctx := context.Background()

	// Simple typed send
	err = fs.Send(ctx, "order.created", OrderCreated{
		OrderID:    "ord-123",
		CustomerID: "cust-456",
		Total:      99.99,
		Currency:   "USD",
		Items: []OrderItem{
			{ProductID: "prod-1", Quantity: 2, Price: 29.99},
			{ProductID: "prod-2", Quantity: 1, Price: 40.01},
		},
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		log.Printf("send failed: %v", err)
	} else {
		fmt.Println("Order created event sent successfully")
	}

	// Send with options
	err = fs.Send(ctx, "order.created", OrderCreated{
		OrderID:    "ord-124",
		CustomerID: "cust-789",
		Total:      149.50,
		Currency:   "EUR",
		Items: []OrderItem{
			{ProductID: "prod-3", Quantity: 1, Price: 149.50},
		},
		CreatedAt: time.Now().UTC(),
	},
		flowrule.WithWorkflowID("wf-abc-123"),
		flowrule.WithCorrelationID("corr-xyz-789"),
		flowrule.WithHeaders(map[string]string{
			"source": "web",
			"version": "v2",
		}),
	)
	if err != nil {
		log.Printf("send with options failed: %v", err)
	}

	// Batch send for high throughput
	orders := []flowrule.SendOptions{
		{RuleSet: "order.created", Payload: OrderCreated{OrderID: "ord-201", CustomerID: "cust-111", Total: 50.00, Currency: "USD", CreatedAt: time.Now().UTC()}},
		{RuleSet: "order.created", Payload: OrderCreated{OrderID: "ord-202", CustomerID: "cust-222", Total: 75.25, Currency: "USD", CreatedAt: time.Now().UTC()}},
		{RuleSet: "order.created", Payload: OrderCreated{OrderID: "ord-203", CustomerID: "cust-333", Total: 120.00, Currency: "USD", CreatedAt: time.Now().UTC()}},
	}

	err = fs.SendBatch(ctx, orders)
	if err != nil {
		log.Printf("batch send failed: %v", err)
	} else {
		fmt.Println("Batch sent successfully")
	}

	// Local embedded mode (no network)
	// cfg := &flowrule.Config{
	//     Transport:   flowrule.TransportLocal,
	//     TenantID:    "acme",
	//     RuleSet:     "order.created",
	//     RuleSource:  ruleYAML, // or RuleSourcePath: "./rules/order.yaml"
	// }
	// fs, _ := flowrule.NewWithConfig(cfg)

	// gRPC mode
	// cfg := &flowrule.Config{
	//     Transport: flowrule.TransportGRPC,
	//     TenantID:  "acme",
	//     GRPCAddr:  "flowrule-server:9090",
	// }

	// HTTP mode
	// cfg := &flowrule.Config{
	//     Transport: flowrule.TransportHTTP,
	//     TenantID:  "acme",
	//     HTTPAddr:  "http://flowrule-server:8080",
	// }
}