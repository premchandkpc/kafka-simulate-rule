package nats

import (
	"context"
	"strings"
	"testing"
)

func TestNewConsumerRejectsFiniteMaxDeliverWithoutRecovery(t *testing.T) {
	_, err := NewConsumer(context.Background(), Config{MaxDeliver: 10})
	if err == nil {
		t.Fatal("NewConsumer() error = nil, want finite MaxDeliver to be rejected")
	}
	if !strings.Contains(err.Error(), "MaxDeliver must be -1 (unlimited)") {
		t.Fatalf("NewConsumer() error = %v, want explicit unlimited-delivery requirement", err)
	}
}
