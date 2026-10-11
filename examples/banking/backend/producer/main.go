package main

import (
	"context"
	"fmt"
	"log"
	"time"

	flowrule "github.com/flowrule/flowrule-sdk/go/flowrule"

	transactioninitiated "examples/banking/generated/go/transaction_initiated"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
	}
}

func run() error {
	client, err := flowrule.New()
	if err != nil {
		return fmt.Errorf("create FlowRule client: %w", err)
	}
	defer client.Close()

	transaction := transactioninitiated.Initiated{
		TransactionId:       "txn-001",
		AccountId:           "account-456",
		Amount:              2500000,
		Currency:            "USD",
		TransactionType:     "wire_transfer",
		CounterpartyAccount: "INTL-789",
		Description:         "INV-2024-001",
		InitiatedAt:         time.Now().UTC().Format(time.RFC3339),
	}

	typed := transactioninitiated.NewClient(client)
	if err := typed.Send(context.Background(), &transaction, flowrule.WithPartitionKey(transaction.AccountId)); err != nil {
		return fmt.Errorf("send transaction.initiated: %w", err)
	}

	fmt.Println("Transaction initiated event published successfully!")
	fmt.Printf("Type: %s\n", transactioninitiated.DefaultRuleSet)
	fmt.Printf("Partition key: %s\n", transaction.AccountId)
	return nil
}
