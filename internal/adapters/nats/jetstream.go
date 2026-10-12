package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	natslib "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Config struct {
	NatsURL        string
	ConnectTimeout time.Duration
	Stream         string
	Consumer       string
	Subjects       []string
	AckWait        time.Duration
	MaxDeliver     int
	NumShards      uint32
}

// Consumer implements both ports.ShardedConsumer and ports.BrokerPublisher
type Consumer struct {
	conn        *natslib.Conn
	js          jetstream.JetStream
	stream      string
	consumers   map[uint32]string // shard -> consumer name
	subjects    []string
	baseDurable string
	cfg         Config
	numShards   uint32
	fetchCursor uint64
	mu          sync.RWMutex
}

func connect(cfg Config) (*natslib.Conn, error) {
	if cfg.ConnectTimeout > 0 {
		return natslib.Connect(cfg.NatsURL, natslib.Timeout(cfg.ConnectTimeout))
	}
	return natslib.Connect(cfg.NatsURL)
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
		cfg.MaxDeliver = -1
	}
	if cfg.MaxDeliver != -1 {
		return nil, fmt.Errorf("NATS MaxDeliver must be -1 (unlimited) until durable exhausted-delivery recovery is implemented")
	}

	conn, err := connect(cfg)
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

// FetchShards fetches at most maxMessages across the requested shards. It
// polls a rotating, bounded subset in parallel so a large shard count cannot
// turn one poll into thousands of sequential fetch timeouts.
func (c *Consumer) FetchShards(ctx context.Context, maxMessages int, shards []uint32) ([]ports.Delivery, error) {
	if maxMessages <= 0 {
		return nil, fmt.Errorf("fetch shards: maxMessages must be > 0")
	}
	if ctx == nil {
		return nil, fmt.Errorf("fetch shards: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		return nil, nil
	}

	shardIDs := append([]uint32(nil), shards...)
	sort.Slice(shardIDs, func(i, j int) bool { return shardIDs[i] < shardIDs[j] })
	unique := shardIDs[:0]
	for _, shardID := range shardIDs {
		if len(unique) == 0 || unique[len(unique)-1] != shardID {
			unique = append(unique, shardID)
		}
	}
	shardIDs = unique
	if len(shardIDs) == 0 {
		return nil, nil
	}

	selectedCount := len(shardIDs)
	if selectedCount > maxMessages {
		selectedCount = maxMessages
	}

	c.mu.Lock()
	shardCount := len(shardIDs)
	// shardCount is bounded by NUM_SHARDS (max 4096), so conversion is safe
	start := int(c.fetchCursor % uint64(shardCount)) //nolint:gosec // shardCount <= 4096
	selected := make([]uint32, selectedCount)
	for i := 0; i < selectedCount; i++ {
		selected[i] = shardIDs[(start+i)%shardCount]
	}
	c.fetchCursor = uint64((start + selectedCount) % shardCount) //nolint:gosec // bounded by shardCount
	c.mu.Unlock()

	type fetchResult struct {
		shard      uint32
		deliveries []ports.Delivery
		err        error
	}
	results := make(chan fetchResult, selectedCount)
	var wg sync.WaitGroup

	baseBudget := maxMessages / selectedCount
	remainder := maxMessages % selectedCount
	for i, shardID := range selected {
		budget := baseBudget
		if i < remainder {
			budget++
		}

		wg.Add(1)
		go func(shardID uint32, budget int) {
			defer wg.Done()

			c.mu.RLock()
			durable := c.consumers[shardID]
			c.mu.RUnlock()
			if durable == "" {
				results <- fetchResult{shard: shardID}
				return
			}

			consumer, err := c.js.Consumer(ctx, c.stream, durable)
			if err != nil {
				results <- fetchResult{shard: shardID, err: fmt.Errorf("get consumer for shard %d: %w", shardID, err)}
				return
			}

			batch, err := consumer.Fetch(budget, jetstream.FetchMaxWait(250*time.Millisecond))
			if err != nil {
				// A fetch timeout is the normal empty-poll path, not a worker error.
				if errors.Is(err, natslib.ErrTimeout) {
					results <- fetchResult{shard: shardID}
					return
				}
				results <- fetchResult{shard: shardID, err: fmt.Errorf("fetch shard %d: %w", shardID, err)}
				return
			}

			deliveries := make([]ports.Delivery, 0, budget)
			for msg := range batch.Messages() {
				deliveries = append(deliveries, &jetstreamDelivery{msg: msg})
			}
			if batchErr := batch.Error(); batchErr != nil &&
				!errors.Is(batchErr, natslib.ErrTimeout) &&
				!errors.Is(batchErr, jetstream.ErrNoMessages) {
				results <- fetchResult{
					shard:      shardID,
					deliveries: deliveries,
					err:        fmt.Errorf("receive messages from shard %d: %w", shardID, batchErr),
				}
				return
			}
			results <- fetchResult{shard: shardID, deliveries: deliveries}
		}(shardID, budget)
	}

	wg.Wait()
	close(results)

	deliveries := make([]ports.Delivery, 0, maxMessages)
	var fetchErrors []error
	for result := range results {
		deliveries = append(deliveries, result.deliveries...)
		if result.err != nil {
			fetchErrors = append(fetchErrors, result.err)
		}
	}

	// Budgets sum to maxMessages, so this is an invariant rather than a
	// truncation step: never discard messages that have already been fetched.
	if len(deliveries) > maxMessages {
		return deliveries, errors.Join(append(fetchErrors, fmt.Errorf("fetch invariant violated: got %d deliveries for limit %d", len(deliveries), maxMessages))...)
	}
	return deliveries, errors.Join(fetchErrors...)
}

func (c *Consumer) OwnedShards() []uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	shards := make([]uint32, 0, len(c.consumers))
	for shardID := range c.consumers {
		shards = append(shards, shardID)
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	return shards
}

func (c *Consumer) ShardCount() uint32 {
	return c.numShards
}

// Ping reports whether the broker connection is up and the event stream exists.
func (c *Consumer) Ping(ctx context.Context) error {
	if c.conn == nil || c.conn.Status() != natslib.CONNECTED {
		return errors.New("nats connection is not established")
	}
	if _, err := c.js.Stream(ctx, c.stream); err != nil {
		return fmt.Errorf("look up stream %q: %w", c.stream, err)
	}
	return nil
}

func (c *Consumer) Close() error {
	c.conn.Close()
	return nil
}

func (c *Consumer) Fetch(ctx context.Context, maxMessages int) ([]ports.Delivery, error) {
	return c.FetchShards(ctx, maxMessages, c.OwnedShards())
}

func (c *Consumer) Subscribe(_ context.Context, _ []string) error {
	return nil
}

func (c *Consumer) Unsubscribe(_ context.Context) error {
	return nil
}

// SendOrderCreate publishes the order-create contract payload to its assigned shard.
// The NATS subject and JetStream acknowledgement details remain inside this adapter.
func (c *Consumer) SendOrderCreate(ctx context.Context, shard uint32, payload []byte) error {
	return c.PublishToShard(ctx, "orders.create", shard, payload)
}

// ConsumeOrderCreate fetches order-create deliveries for the requested owned shards.
// It returns ports.Delivery values so callers never need JetStream message types.
func (c *Consumer) ConsumeOrderCreate(ctx context.Context, maxMessages int, shards []uint32) ([]ports.Delivery, error) {
	return c.FetchShards(ctx, maxMessages, shards)
}

// Compile-time assertions keep the adapter aligned with consumer-owned contracts.
var _ ports.EventConsumer = (*Consumer)(nil)
var _ ports.EventPublisher = (*Consumer)(nil)
var _ ports.OrderEventBroker = (*Consumer)(nil)
var _ ports.EventPublisher = (*Publisher)(nil)

// Publish is the generic adapter-level primitive that implements ports.BrokerPublisher.
func (c *Consumer) Publish(ctx context.Context, subject string, data []byte) error {
	_, err := c.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("publish subject %q: %w", subject, err)
	}
	return nil
}

// PublishBatch implements ports.BrokerPublisher.
func (c *Consumer) PublishBatch(ctx context.Context, messages []ports.PublishMessage) error {
	for i, msg := range messages {
		if _, err := c.js.Publish(ctx, msg.Subject, msg.Data); err != nil {
			return fmt.Errorf("publish batch message %d to %q: %w", i, msg.Subject, err)
		}
	}
	return nil
}

// PublishToShard implements ports.BrokerPublisher.
func (c *Consumer) PublishToShard(ctx context.Context, baseSubject string, shard uint32, data []byte) error {
	subject := fmt.Sprintf("%s.shard.%d", baseSubject, shard)
	return c.Publish(ctx, subject, data)
}

// Publisher provides a dedicated publisher for sending messages without consuming
type Publisher struct {
	conn      *natslib.Conn
	js        jetstream.JetStream
	stream    string
	numShards uint32
}

func NewPublisher(cfg Config) (*Publisher, error) {
	if cfg.NumShards == 0 {
		cfg.NumShards = 4096
	}
	conn, err := connect(cfg)
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
		return fmt.Errorf("publish subject %q: %w", subject, err)
	}
	return nil
}

// SendOrderCreate publishes the order-create contract payload to its assigned shard.
func (p *Publisher) SendOrderCreate(ctx context.Context, shard uint32, payload []byte) error {
	return p.PublishToShard(ctx, "orders.create", shard, payload)
}

func (p *Publisher) PublishToShard(ctx context.Context, baseSubject string, shard uint32, data []byte) error {
	subject := fmt.Sprintf("%s.shard.%d", baseSubject, shard)
	return p.Publish(ctx, subject, data)
}

func (p *Publisher) PublishBatch(ctx context.Context, messages []ports.PublishMessage) error {
	for i, msg := range messages {
		if err := p.Publish(ctx, msg.Subject, msg.Data); err != nil {
			return fmt.Errorf("publish batch message %d: %w", i, err)
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

func (d *jetstreamDelivery) Attempt() uint64 {
	metadata, err := d.msg.Metadata()
	if err != nil || metadata == nil || metadata.NumDelivered == 0 {
		return 1
	}
	return metadata.NumDelivered
}

func (d *jetstreamDelivery) Ack(_ context.Context) error {
	return d.msg.Ack()
}

func (d *jetstreamDelivery) Nak(_ context.Context) error {
	return d.msg.Nak()
}

func (d *jetstreamDelivery) Retry(_ context.Context, delay time.Duration) error {
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
