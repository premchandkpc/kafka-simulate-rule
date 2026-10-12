package ports

import (
	"context"
)

// PubSub provides publish-subscribe messaging
type PubSub interface {
	Publish(ctx context.Context, channel string, message []byte) error
	Subscribe(ctx context.Context, channel string) (<-chan PubSubMessage, error)
	Unsubscribe(ctx context.Context, channel string) error
	Close() error
}

type PubSubMessage struct {
	Channel string
	Message []byte
	Pattern string
}
