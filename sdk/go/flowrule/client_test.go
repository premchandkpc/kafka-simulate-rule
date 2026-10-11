package flowrule

import (
	"context"
	"testing"
	"time"
)

func TestSubscribeRejectsUnsupportedTransport(t *testing.T) {
	client := &Client{transport: &httpTransport{}}

	err := client.Subscribe(context.Background(), "order.created", func(context.Context, *Envelope) error {
		return nil
	})
	if err != ErrSubscriptionsUnsupported {
		t.Fatalf("Subscribe() error = %v, want %v", err, ErrSubscriptionsUnsupported)
	}
}

func TestCloseCancelsActiveSubscription(t *testing.T) {
	lifecycleCtx, lifecycleClose := context.WithCancel(context.Background())
	transport := &subscriptionTestTransport{started: make(chan struct{})}
	client := &Client{
		transport:      transport,
		lifecycleCtx:   lifecycleCtx,
		lifecycleClose: lifecycleClose,
	}

	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- client.Subscribe(context.Background(), "order.created", func(context.Context, *Envelope) error {
			return nil
		})
	}()

	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("subscription did not start")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case err := <-subscribeDone:
		if err != context.Canceled {
			t.Fatalf("Subscribe() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() did not cancel the active subscription")
	}
}

type subscriptionTestTransport struct {
	started chan struct{}
}

func (t *subscriptionTestTransport) Send(context.Context, *Envelope) error { return nil }

func (t *subscriptionTestTransport) SendBatch(context.Context, []*Envelope) error { return nil }

func (t *subscriptionTestTransport) Close() error { return nil }

func (t *subscriptionTestTransport) Name() string { return "test" }

func (t *subscriptionTestTransport) Subscribe(ctx context.Context, _ string, _ EventHandler) error {
	close(t.started)
	<-ctx.Done()
	return ctx.Err()
}
