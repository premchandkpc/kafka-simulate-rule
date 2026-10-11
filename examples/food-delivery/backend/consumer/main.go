package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	flowrule "github.com/flowrule/flowrule-sdk/go/flowrule"

	orderplaced "examples/food-delivery/generated/go/order_placed"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
	}
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	client, err := flowrule.New()
	if err != nil {
		return fmt.Errorf("create FlowRule client: %w", err)
	}
	defer client.Close()

	typed := orderplaced.NewClient(client)
	log.Printf("Listening for %s events for tenant %q", orderplaced.DefaultRuleSet, os.Getenv("FLOWRULE_TENANT_ID"))
	err = typed.Subscribe(ctx, func(_ context.Context, order *orderplaced.Placed, envelope *flowrule.Envelope) error {
		log.Printf(
			"Received %s event id=%s partition_key=%s order_id=%s customer_id=%s restaurant_id=%s total=%.2f %s",
			envelope.Type,
			envelope.ID,
			envelope.PartitionKey,
			order.OrderId,
			order.CustomerId,
			order.RestaurantId,
			order.TotalAmount/100,
			order.Currency,
		)
		return nil
	})
	if errors.Is(err, context.Canceled) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("subscribe to order.placed: %w", err)
	}
	return nil
}
