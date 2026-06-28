package transport

import (
	"context"
	"log"
)

type Producer struct {
	topic string
}

func NewProducer(topic string) *Producer {
	return &Producer{topic: topic}
}

func (p *Producer) Send(ctx context.Context, key, value []byte) error {
	log.Printf("produced to %s: key=%s val=%d bytes", p.topic, string(key), len(value))
	return nil
}

func (p *Producer) Close() {}
