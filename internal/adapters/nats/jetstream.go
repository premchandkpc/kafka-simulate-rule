package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Config struct {
	NatsURL    string
	Stream     string
	Consumer   string
	Subjects   []string
	AckWait    time.Duration
	MaxDeliver int
	NumShards  uint32
}

// Consumer implements both ports.ShardedConsumer and ports.BrokerPublisher
type Consumer struct {
	conn        *nats.Conn
	js          jetstream.JetStream
	stream      string
	consumers   map[uint32]string // shard -> consumer name
	subjects    []string
	baseDurable string
	cfg         Config
	numShards   uint32
	mu          sync.RWMutex
}

func NewConsumer(ctx context.Context, cfg Config) (*Consumer, error) {
	if cfg.NumShards == 0 {
		cfg.NumShards = 4096
	}
	if len(cfg.Subjects) == 0 {
		cfg.Subjects = []string{"events"}
	}
	if cfg.AckWait == 0 {
		cfg.AckWait = 30 * time.Second
	}
	if cfg.MaxDeliver == 0 {
		cfg.MaxDeliver = 10
	}

	conn, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create jetstream: %w", err)
	}

	// Ensure stream exists with sharded subjects
	shardedSubjects := make([]string, 0, len(cfg.Subjects)*int(cfg.NumShards))
	for _, s := range cfg.Subjects {
		for shard := uint32(0); shard < cfg.NumShards; shard++ {
			shardedSubjects = append(shardedSubjects, fmt.Sprintf("%s.shard.%d", s, shard))
		}
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      cfg.Stream,
		Subjects:  shardedSubjects,
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
		MaxMsgs:   -1,
		Discard:   jetstream.DiscardOld,
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create stream: %w", err)
	}

	baseDurable := cfg.Consumer
	if baseDurable == "" {
		baseDurable = "flowrule-worker"
	}

	c := &Consumer{
		conn:        conn,
		js:          js,
		stream:      cfg.Stream,
		consumers:   make(map[uint32]string),
		subjects:    cfg.Subjects,
		baseDurable: baseDurable,
		cfg:         cfg,
		numShards:   cfg.NumShards,
	}
	return c, nil
}

func (c *Consumer) EnsureShardConsumer(ctx context.Context, shard uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.consumers[shard]; ok {
		return nil
	}

	durable := fmt.Sprintf("%s-shard-%d", c.baseDurable, shard)
	filterSubject := fmt.Sprintf("%s.shard.%d", c.cfg.Subjects[0], shard)

	_, err := c.js.CreateOrUpdateConsumer(ctx, c.cfg.Stream, jetstream.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: filterSubject,
		MaxDeliver:    c.cfg.MaxDeliver,
		AckWait:       c.cfg.AckWait,
	})
	if err != nil {
		return fmt.Errorf("create consumer for shard %d: %w", shard, err)
	}

	c.consumers[shard] = durable
	log.Printf("created consumer for shard %d: %s", shard, durable)
	return nil
}

func (c *Consumer) RemoveShardConsumer(ctx context.Context, shard uint32) error {
	c.mu.Lock()
	durable, ok := c.consumers[shard]
	c.mu.Unlock()

	if !ok {
		return nil
	}

	err := c.js.DeleteConsumer(ctx, c.cfg.Stream, durable)
	if err != nil {
		return fmt.Errorf("delete consumer for shard %d: %w", shard, err)
	}

	c.mu.Lock()
	delete(c.consumers, shard)
	c.mu.Unlock()
	log.Printf("removed consumer for shard %d", shard)
	return nil
}

func (c *Consumer) FetchShards(ctx context.Context, maxMessages int, shards []uint32) ([]ports.Delivery, error) {
	if len(shards) == 0 {
		return nil, nil
	}

	var allDeliveries []ports.Delivery
	perShard := maxMessages / len(shards)
	if perShard == 0 {
		perShard = 1
	}

	for _, shard := range shards {
		c.mu.RLock()
		durable := c.consumers[shard]
		c.mu.RUnlock()

		if durable == "" {
			continue
		}

		consumer, err := c.js.Consumer(ctx, c.stream, durable)
		if err != nil {
			log.Printf("get consumer for shard %d: %v", shard, err)
			continue
		}

		batch, err := consumer.Fetch(perShard, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			log.Printf("fetch from shard %d: %v", shard, err)
			continue
		}

		for msg := range batch.Messages() {
			allDeliveries = append(allDeliveries, &jetstreamDelivery{msg: msg})
		}
	}

	return allDeliveries, nil
}

func (c *Consumer) OwnedShards() []uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	shards := make([]uint32, 0, len(c.consumers))
	for shard := range c.consumers {
		shards = append(shards, shard)
	}
	return shards
}

func (c *Consumer) ShardCount() uint32 {
	return c.numShards
}

func (c *Consumer) Close() error {
	c.conn.Close()
	return nil
}

func (c *Consumer) Fetch(ctx context.Context, maxMessages int) ([]ports.Delivery, error) {
	return c.FetchShards(ctx, maxMessages, c.OwnedShards())
}

func (c *Consumer) Subscribe(ctx context.Context, subjects []string) error {
	return nil
}

func (c *Consumer) Unsubscribe(ctx context.Context) error {
	return nil
}

// Publish implements ports.BrokerPublisher
func (c *Consumer) Publish(ctx context.Context, subject string, data []byte) error {
	_, err := c.js.Publish(ctx, subject, data)
	return err
}

// PublishBatch implements ports.BrokerPublisher
func (c *Consumer) PublishBatch(ctx context.Context, messages []ports.PublishMessage) error {
	for _, msg := range messages {
		_, err := c.js.Publish(ctx, msg.Subject, msg.Data)
		if err != nil {
			return err
		}
	}
	return nil
}

// PublishToShard implements ports.BrokerPublisher
func (c *Consumer) PublishToShard(ctx context.Context, baseSubject string, shard uint32, data []byte) error {
	subject := fmt.Sprintf("%s.shard.%d", baseSubject, shard)
	_, err := c.js.Publish(ctx, subject, data)
	return err
}

// Publisher provides a dedicated publisher for sending messages without consuming
type Publisher struct {
	conn      *nats.Conn
	js        jetstream.JetStream
	stream    string
	numShards uint32
}

func NewPublisher(cfg Config) (*Publisher, error) {
	if cfg.NumShards == 0 {
		cfg.NumShards = 4096
	}
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
		conn:      conn,
		js:        js,
		stream:    cfg.Stream,
		numShards: cfg.NumShards,
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, subject string, data []byte) error {
	_, err := p.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}

func (p *Publisher) PublishToShard(ctx context.Context, baseSubject string, shard uint32, data []byte) error {
	subject := fmt.Sprintf("%s.shard.%d", baseSubject, shard)
	_, err := p.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("publish to shard %d: %w", shard, err)
	}
	return nil
}

func (p *Publisher) PublishBatch(ctx context.Context, messages []ports.PublishMessage) error {
	for _, msg := range messages {
		_, err := p.js.Publish(ctx, msg.Subject, msg.Data)
		if err != nil {
			return fmt.Errorf("publish batch: %w", err)
		}
	}
	return nil
}

func (p *Publisher) Close() error {
	p.conn.Close()
	return nil
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
