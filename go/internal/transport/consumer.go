package transport

import (
	"context"
	"log"
)

type Handler func(ctx context.Context, msg []byte) ([]byte, error)

type Consumer struct {
	handler Handler
	topic   string
	msgCh   chan []byte
	stopCh  chan struct{}
}

func NewConsumer(topic string, handler Handler) *Consumer {
	return &Consumer{
		handler: handler,
		topic:   topic,
		msgCh:   make(chan []byte, 100),
		stopCh:  make(chan struct{}),
	}
}

func (c *Consumer) Start(ctx context.Context) {
	log.Printf("consumer started for topic: %s", c.topic)
	for {
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		case msg := <-c.msgCh:
			_, err := c.handler(ctx, msg)
			if err != nil {
				log.Printf("handler error: %v", err)
			}
		}
	}
}

func (c *Consumer) Inject(msg []byte) {
	select {
	case c.msgCh <- msg:
	default:
	}
}

func (c *Consumer) Stop() {
	close(c.stopCh)
}
