package transport

import (
	"context"
	"testing"
)

func TestConsumerStartStop(t *testing.T) {
	c := NewConsumer("test-topic", func(ctx context.Context, msg []byte) ([]byte, error) {
		return nil, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.Start(ctx)
}

func TestConsumerHandlerCalled(t *testing.T) {
	done := make(chan struct{})
	c := NewConsumer("test-topic", func(ctx context.Context, msg []byte) ([]byte, error) {
		if string(msg) == "hello" {
			close(done)
		}
		return nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Start(ctx)
	c.Inject([]byte("hello"))
	<-done
}

func TestConsumerStopCh(t *testing.T) {
	c := NewConsumer("test-topic", func(ctx context.Context, msg []byte) ([]byte, error) {
		return nil, nil
	})

	go func() {
		c.Inject([]byte("msg"))
	}()
	c.Stop()
}
