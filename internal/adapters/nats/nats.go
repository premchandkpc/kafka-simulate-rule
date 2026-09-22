package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Consumer struct {
	conn      *nats.Conn
	js        jetstream.JetStream
	stream    string
	consumer  string
	subjects  []string
}

type Config struct {
	NatsURL    string
	Stream     string
	Consumer   string
	Subjects   []string
	AckWait    time.Duration
	MaxDeliver int
}

func NewConsumer(ctx context.Context, cfg Config) (*Consumer, error) {
	conn, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create jetstream: %w", err)
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      cfg.Stream,
		Subjects:  cfg.Subjects,
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
		MaxMsgs:   -1,
		Discard:   jetstream.DiscardOld,
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create stream: %w", err)
	}

	durable := cfg.Consumer
	if durable == "" {
		durable = "flowrule-worker"
	}

	_, err = js.CreateOrUpdateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: cfg.Subjects[0],
		MaxDeliver:    cfg.MaxDeliver,
		AckWait:       cfg.AckWait,
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	return &Consumer{
		conn:     conn,
		js:       js,
		stream:   cfg.Stream,
		consumer: durable,
	}, nil
}

func (c *Consumer) Fetch(ctx context.Context, maxMessages int) ([]ports.Delivery, error) {
	consumer, err := c.js.Consumer(ctx, c.stream, c.consumer)
	if err != nil {
		return nil, fmt.Errorf("get consumer: %w", err)
	}

	batch, err := consumer.Fetch(maxMessages, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}

	var deliveries []ports.Delivery
	for msg := range batch.Messages() {
		deliveries = append(deliveries, &jetstreamDelivery{msg: msg})
	}
	return deliveries, nil
}

func (c *Consumer) Close() {
	c.conn.Close()
}

type jetstreamDelivery struct {
	msg jetstream.Msg
}

func (d *jetstreamDelivery) Event() (*domain.EventEnvelope, error) {
	env := &domain.EventEnvelope{}
	if err := json.Unmarshal(d.msg.Data(), env); err != nil {
		return nil, err
	}
	return env, nil
}

func (d *jetstreamDelivery) Raw() []byte {
	return d.msg.Data()
}

func (d *jetstreamDelivery) Ack(ctx context.Context) error {
	return d.msg.Ack()
}

func (d *jetstreamDelivery) Nak(ctx context.Context) error {
	return d.msg.Nak()
}

func (d *jetstreamDelivery) Retry(ctx context.Context, delay time.Duration) error {
	return d.msg.NakWithDelay(delay)
}

type Publisher struct {
	conn   *nats.Conn
	js     jetstream.JetStream
	stream string
}

func NewPublisher(cfg Config) (*Publisher, error) {
	conn, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create jetstream: %w", err)
	}

	return &Publisher{
		conn:   conn,
		js:     js,
		stream: cfg.Stream,
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, subject string, data []byte) error {
	_, err := p.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}

func (p *Publisher) Close() {
	p.conn.Close()
}