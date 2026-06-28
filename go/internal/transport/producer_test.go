package transport

import (
	"context"
	"testing"
)

func TestProducerSendClose(t *testing.T) {
	p := NewProducer("test-topic")
	err := p.Send(context.Background(), []byte("key"), []byte("value"))
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	p.Close()
}
