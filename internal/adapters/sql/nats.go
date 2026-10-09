package sql

import (
	"context"
	"encoding/json"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// JetStreamConsumer wraps jetstream.JetStream to implement ports.BrokerConsumer
type JetStreamConsumer struct {
	js        jetstream.JetStream
	nc        *nats.Conn
	stream    string
	consumers map[uint32]string // shard -> consumer name
	numShards uint32
}

func NewJetStreamConsumer(ctx context.Context, natsURL, stream string, numShards uint32) (*JetStreamConsumer, error) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}

	// Ensure stream exists
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     stream,
		Subjects: []string{"events.>"},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		nc.Close()
		return nil, err
	}

	return &JetStreamConsumer{
		js:        js,
		nc:        nc,
		stream:    stream,
		consumers: make(map[uint32]string),
		numShards: numShards,
	}, nil
}

func (c *JetStreamConsumer) Fetch(ctx context.Context, maxMessages int) ([]ports.Delivery, error) {
	return c.FetchShards(ctx, maxMessages, c.OwnedShards())
}

func (c *JetStreamConsumer) FetchShards(ctx context.Context, maxMessages int, shards []uint32) ([]ports.Delivery, error) {
	if len(shards) == 0 {
		return nil, nil
	}

	var allDeliveries []ports.Delivery
	perShard := maxMessages / len(shards)
	if perShard == 0 {
		perShard = 1
	}

	for _, shard := range shards {
		durable := c.consumers[shard]
		if durable == "" {
			continue
		}

		consumer, err := c.js.Consumer(ctx, c.stream, durable)
		if err != nil {
			continue
		}

		batch, err := consumer.Fetch(perShard, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			continue
		}

		for msg := range batch.Messages() {
			allDeliveries = append(allDeliveries, &jetstreamDelivery{msg: msg})
		}
	}

	return allDeliveries, nil
}

func (c *JetStreamConsumer) Subscribe(ctx context.Context, subjects []string) error {
	return nil
}

func (c *JetStreamConsumer) Unsubscribe(ctx context.Context) error {
	return nil
}

func (c *JetStreamConsumer) Close() error {
	c.nc.Close()
	return nil
}

func (c *JetStreamConsumer) EnsureShardConsumer(ctx context.Context, shard uint32) error {
	if _, ok := c.consumers[shard]; ok {
		return nil
	}

	durable := "flowrule-worker-shard-" + string(rune('0'+shard%10))
	filterSubject := "events.shard." + string(rune('0'+shard%10))

	_, err := c.js.CreateOrUpdateConsumer(ctx, c.stream, jetstream.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: filterSubject,
		MaxDeliver:    10,
		AckWait:       30 * time.Second,
	})
	if err != nil {
		return err
	}

	c.consumers[shard] = durable
	return nil
}

func (c *JetStreamConsumer) RemoveShardConsumer(ctx context.Context, shard uint32) error {
	durable, ok := c.consumers[shard]
	if !ok {
		return nil
	}

	err := c.js.DeleteConsumer(ctx, c.stream, durable)
	if err != nil {
		return err
	}

	delete(c.consumers, shard)
	return nil
}

func (c *JetStreamConsumer) OwnedShards() []uint32 {
	shards := make([]uint32, 0, len(c.consumers))
	for shard := range c.consumers {
		shards = append(shards, shard)
	}
	return shards
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

func (d *jetstreamDelivery) Headers() map[string]string {
	h := make(map[string]string)
	for k, v := range d.msg.Headers() {
		if len(v) > 0 {
			h[k] = v[0]
		}
	}
	return h
}

func (d *jetstreamDelivery) Subject() string {
	return d.msg.Subject()
}

// Publish implements ports.BrokerPublisher
func (c *JetStreamConsumer) Publish(ctx context.Context, subject string, data []byte) error {
	_, err := c.js.Publish(ctx, subject, data)
	return err
}

// PublishBatch implements ports.BrokerPublisher
func (c *JetStreamConsumer) PublishBatch(ctx context.Context, messages []ports.PublishMessage) error {
	for _, msg := range messages {
		_, err := c.js.Publish(ctx, msg.Subject, msg.Data)
		if err != nil {
			return err
		}
	}
	return nil
}

// PublishToShard implements ports.BrokerPublisher
func (c *JetStreamConsumer) PublishToShard(ctx context.Context, baseSubject string, shard uint32, data []byte) error {
	subject := baseSubject + ".shard." + string(rune('0'+shard%10))
	_, err := c.js.Publish(ctx, subject, data)
	return err
}