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

	transactioninitiated "github.com/flowrule/domains/banking/builds/go/transaction_initiated"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
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

	typed := transactioninitiated.NewClient(client)
	log.Printf("Listening for %s events for tenant %q", transactioninitiated.DefaultRuleSet, os.Getenv("FLOWRULE_TENANT_ID"))
	err = typed.Subscribe(ctx, func(_ context.Context, transaction *transactioninitiated.Initiated, envelope *flowrule.Envelope) error {
		log.Printf(
			"Received %s event id=%s partition_key=%s transaction_id=%s account_id=%s amount=%.2f %s",
			envelope.Type,
			envelope.ID,
			envelope.PartitionKey,
			transaction.TransactionId,
			transaction.AccountId,
			transaction.Amount/100,
			transaction.Currency,
		)
		return nil
	})
	if errors.Is(err, context.Canceled) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("subscribe to transaction.initiated: %w", err)
	}
	return nil
}
