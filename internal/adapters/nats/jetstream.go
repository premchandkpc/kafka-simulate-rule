package nats

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
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
	RuleSets       []string
	AckWait        time.Duration
	MaxDeliver     int
	NumShards      uint32
}

type consumerKey struct {
	baseSubject string
	ruleSet     string
	shard       uint32
}

// Consumer is the pull-consumer adapter. Publisher is composed separately so
// consuming and publishing lifecycles can be managed independently.
type Consumer struct {
	conn        *natslib.Conn
	js          jetstream.JetStream
	stream      string
	consumers   map[consumerKey]string
	subjects    []string
	ruleSets    []string
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

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.NumShards == 0 {
		cfg.NumShards = 4096
	}
	if len(cfg.Subjects) == 0 {
		cfg.Subjects = []string{"events"}
	}
	if cfg.Stream == "" {
		cfg.Stream = "flowrule"
	}
	if cfg.Consumer == "" {
		cfg.Consumer = "flowrule-worker"
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = 30 * time.Second
	}
	if cfg.MaxDeliver == 0 {
		cfg.MaxDeliver = -1
	}
	if cfg.MaxDeliver != -1 {
		return Config{}, fmt.Errorf("NATS MaxDeliver must be -1 (unlimited) until durable exhausted-delivery recovery is implemented")
	}

	subjects := make([]string, 0, len(cfg.Subjects))
	seenSubjects := make(map[string]struct{}, len(cfg.Subjects))
	for _, subject := range cfg.Subjects {
		subject = strings.TrimSpace(subject)
		if subject == "" {
			return Config{}, fmt.Errorf("NATS subject must not be empty")
		}
		if _, exists := seenSubjects[subject]; exists {
			continue
		}
		seenSubjects[subject] = struct{}{}
		subjects = append(subjects, subject)
	}
	cfg.Subjects = subjects

	ruleSets := make([]string, 0, len(cfg.RuleSets))
	seenRuleSets := make(map[string]struct{}, len(cfg.RuleSets))
	for _, ruleSet := range cfg.RuleSets {
		ruleSet = strings.TrimSpace(ruleSet)
		if ruleSet == "" {
			return Config{}, fmt.Errorf("NATS ruleset must not be empty")
		}
		if len(ruleSet) > 128 {
			return Config{}, fmt.Errorf("NATS ruleset exceeds 128 bytes")
		}
		if _, exists := seenRuleSets[ruleSet]; exists {
			return Config{}, fmt.Errorf("duplicate NATS ruleset %q", ruleSet)
		}
		seenRuleSets[ruleSet] = struct{}{}
		ruleSets = append(ruleSets, ruleSet)
	}
	sort.Strings(ruleSets)
	cfg.RuleSets = ruleSets
	return cfg, nil
}

func streamSubjects(cfg Config) []string {
	subjects := make([]string, 0, len(cfg.Subjects)*2)
	for _, base := range cfg.Subjects {
		subjects = append(subjects, base+".shard.*", base+".ruleset.*.shard.*")
	}
	return subjects
}

func ensureStream(ctx context.Context, js jetstream.JetStream, cfg Config) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: cfg.Stream, Subjects: streamSubjects(cfg), Retention: jetstream.WorkQueuePolicy,
		Storage: jetstream.FileStorage, MaxMsgs: -1, Discard: jetstream.DiscardOld,
	})
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	return nil
}

func NewConsumer(ctx context.Context, cfg Config) (*Consumer, error) {
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
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
	if err := ensureStream(ctx, js, cfg); err != nil {
		conn.Close()
		return nil, err
	}
	return &Consumer{
		conn: conn, js: js, stream: cfg.Stream, consumers: make(map[consumerKey]string),
		subjects: cfg.Subjects, ruleSets: cfg.RuleSets, baseDurable: cfg.Consumer,
		cfg: cfg, numShards: cfg.NumShards,
	}, nil
}

func ruleSetToken(ruleSet string) string {
	sum := sha256.Sum256([]byte(ruleSet))
	return hex.EncodeToString(sum[:])
}

func ruleSetSubject(baseSubject, ruleSet string, shard uint32) string {
	return fmt.Sprintf("%s.ruleset.%s.shard.%d", baseSubject, ruleSetToken(ruleSet), shard)
}

func containsRuleSet(ruleSets []string, candidate string) bool {
	for _, ruleSet := range ruleSets {
		if ruleSet == candidate {
			return true
		}
	}
	return false
}

func subjectForMessage(baseSubject string, shard uint32, data []byte, ruleSets []string) string {
	var envelope domain.EventEnvelope
	if json.Unmarshal(data, &envelope) == nil && envelope.Type != "" && containsRuleSet(ruleSets, envelope.Type) {
		return ruleSetSubject(baseSubject, envelope.Type, shard)
	}
	return fmt.Sprintf("%s.shard.%d", baseSubject, shard)
}

func safeDurableToken(value string) string {
	value = strings.NewReplacer(".", "-", "*", "x", ">", "x", " ", "-").Replace(value)
	if len(value) > 32 {
		value = value[:32]
	}
	return value
}

func (c *Consumer) ensureConsumerLocked(ctx context.Context, key consumerKey) error {
	if _, exists := c.consumers[key]; exists {
		return nil
	}
	filterSubject := fmt.Sprintf("%s.shard.%d", key.baseSubject, key.shard)
	durable := fmt.Sprintf("%s-shard-%d", c.baseDurable, key.shard)
	if key.baseSubject != c.subjects[0] {
		durable = fmt.Sprintf("%s-%s-shard-%d", c.baseDurable, safeDurableToken(key.baseSubject), key.shard)
	}
	if key.ruleSet != "" {
		token := ruleSetToken(key.ruleSet)
		filterSubject = ruleSetSubject(key.baseSubject, key.ruleSet, key.shard)
		durable = fmt.Sprintf("%s-%s-rs-%s-shard-%d", c.baseDurable, safeDurableToken(key.baseSubject), token[:12], key.shard)
	}
	_, err := c.js.CreateOrUpdateConsumer(ctx, c.cfg.Stream, jetstream.ConsumerConfig{
		Durable: durable, AckPolicy: jetstream.AckExplicitPolicy, FilterSubject: filterSubject,
		MaxDeliver: c.cfg.MaxDeliver, AckWait: c.cfg.AckWait,
	})
	if err != nil {
		return fmt.Errorf("create consumer for subject %q shard %d: %w", filterSubject, key.shard, err)
	}
	c.consumers[key] = durable
	log.Printf("created durable consumer subject=%s ruleset=%q shard=%d durable=%s", key.baseSubject, key.ruleSet, key.shard, durable)
	return nil
}

func (c *Consumer) EnsureShardConsumer(ctx context.Context, shard uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, base := range c.subjects {
		if err := c.ensureConsumerLocked(ctx, consumerKey{baseSubject: base, shard: shard}); err != nil {
			return err
		}
		for _, ruleSet := range c.ruleSets {
			if err := c.ensureConsumerLocked(ctx, consumerKey{baseSubject: base, ruleSet: ruleSet, shard: shard}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Consumer) RemoveShardConsumer(ctx context.Context, shard uint32) error {
	c.mu.RLock()
	keys := make([]consumerKey, 0)
	for key := range c.consumers {
		if key.shard == shard {
			keys = append(keys, key)
		}
	}
	c.mu.RUnlock()
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].baseSubject != keys[j].baseSubject {
			return keys[i].baseSubject < keys[j].baseSubject
		}
		return keys[i].ruleSet < keys[j].ruleSet
	})
	var removeErrs []error
	for _, key := range keys {
		c.mu.RLock()
		durable := c.consumers[key]
		c.mu.RUnlock()
		if durable == "" {
			continue
		}
		if err := c.js.DeleteConsumer(ctx, c.cfg.Stream, durable); err != nil {
			removeErrs = append(removeErrs, fmt.Errorf("delete consumer %q: %w", durable, err))
			continue
		}
		c.mu.Lock()
		if c.consumers[key] == durable {
			delete(c.consumers, key)
		}
		c.mu.Unlock()
	}
	return errors.Join(removeErrs...)
}

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
	wanted := make(map[uint32]struct{}, len(shards))
	for _, shard := range shards {
		wanted[shard] = struct{}{}
	}

	c.mu.Lock()
	keys := make([]consumerKey, 0, len(c.consumers))
	for key := range c.consumers {
		if _, ok := wanted[key.shard]; ok {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].shard != keys[j].shard {
			return keys[i].shard < keys[j].shard
		}
		if keys[i].baseSubject != keys[j].baseSubject {
			return keys[i].baseSubject < keys[j].baseSubject
		}
		return keys[i].ruleSet < keys[j].ruleSet
	})
	if len(keys) == 0 {
		c.mu.Unlock()
		return nil, nil
	}
	selectedCount := len(keys)
	if selectedCount > maxMessages {
		selectedCount = maxMessages
	}
	start := int(c.fetchCursor % uint64(len(keys)))
	selected := make([]consumerKey, selectedCount)
	durables := make(map[consumerKey]string, selectedCount)
	for i := 0; i < selectedCount; i++ {
		selected[i] = keys[(start+i)%len(keys)]
		durables[selected[i]] = c.consumers[selected[i]]
	}
	c.fetchCursor = uint64((start + selectedCount) % len(keys))
	c.mu.Unlock()

	type fetchResult struct {
		deliveries []ports.Delivery
		err        error
	}
	results := make(chan fetchResult, selectedCount)
	var wg sync.WaitGroup
	baseBudget, remainder := maxMessages/selectedCount, maxMessages%selectedCount
	for i, key := range selected {
		budget := baseBudget
		if i < remainder {
			budget++
		}
		wg.Add(1)
		go func(key consumerKey, durable string, budget int) {
			defer wg.Done()
			consumer, err := c.js.Consumer(ctx, c.stream, durable)
			if err != nil {
				results <- fetchResult{err: fmt.Errorf("get consumer %q: %w", durable, err)}
				return
			}
			batch, err := consumer.Fetch(budget, jetstream.FetchMaxWait(250*time.Millisecond))
			if err != nil {
				if errors.Is(err, natslib.ErrTimeout) {
					results <- fetchResult{}
					return
				}
				results <- fetchResult{err: fmt.Errorf("fetch consumer %q: %w", durable, err)}
				return
			}
			deliveries := make([]ports.Delivery, 0, budget)
			for msg := range batch.Messages() {
				deliveries = append(deliveries, &jetstreamDelivery{msg: msg})
			}
			if batchErr := batch.Error(); batchErr != nil &&
				!errors.Is(batchErr, natslib.ErrTimeout) &&
				!errors.Is(batchErr, jetstream.ErrNoMessages) {
				results <- fetchResult{deliveries: deliveries, err: fmt.Errorf("receive from consumer %q: %w", durable, batchErr)}
				return
			}
			results <- fetchResult{deliveries: deliveries}
		}(key, durables[key], budget)
	}
	wg.Wait()
	close(results)
	deliveries := make([]ports.Delivery, 0, maxMessages)
	var fetchErrs []error
	for result := range results {
		deliveries = append(deliveries, result.deliveries...)
		if result.err != nil {
			fetchErrs = append(fetchErrs, result.err)
		}
	}
	if len(deliveries) > maxMessages {
		return deliveries, errors.Join(append(fetchErrs, fmt.Errorf("fetch invariant violated: got %d deliveries for limit %d", len(deliveries), maxMessages))...)
	}
	return deliveries, errors.Join(fetchErrs...)
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
	expected := len(c.subjects) * (1 + len(c.ruleSets))
	counts := make(map[uint32]int)
	for key := range c.consumers {
		counts[key.shard]++
	}
	shards := make([]uint32, 0, len(counts))
	for shard, count := range counts {
		if count == expected {
			shards = append(shards, shard)
		}
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
var _ ports.ShardedConsumer = (*Consumer)(nil)
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
	return c.Publish(ctx, subjectForMessage(baseSubject, shard, data, c.ruleSets), data)
}

// Publisher provides a dedicated publisher for sending messages without consuming
type Publisher struct {
	conn      *natslib.Conn
	js        jetstream.JetStream
	stream    string
	numShards uint32
	ruleSets  []string
}

func NewPublisher(cfg Config) (*Publisher, error) {
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := connect(cfg)
	if err != nil {
		return nil, fmt.Errorf("connect nats publisher: %w", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create jetstream publisher: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ensureStream(ctx, js, cfg); err != nil {
		conn.Close()
		return nil, err
	}
	return &Publisher{
		conn: conn, js: js, stream: cfg.Stream, numShards: cfg.NumShards, ruleSets: cfg.RuleSets,
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
	return p.Publish(ctx, subjectForMessage(baseSubject, shard, data, p.ruleSets), data)
}

func (p *Publisher) PublishBatch(ctx context.Context, messages []ports.PublishMessage) error {
	for i, msg := range messages {
		if err := p.Publish(ctx, msg.Subject, msg.Data); err != nil {
			return fmt.Errorf("publish batch message %d: %w", i, err)
		}
	}
	return nil
}

func (p *Publisher) Ping(ctx context.Context) error {
	if p.conn == nil || p.conn.Status() != natslib.CONNECTED {
		return errors.New("nats publisher connection is not established")
	}
	if _, err := p.js.Stream(ctx, p.stream); err != nil {
		return fmt.Errorf("look up stream %q: %w", p.stream, err)
	}
	return nil
}

func (p *Publisher) Ping(ctx context.Context) error {
	if p.conn == nil || p.conn.Status() != natslib.CONNECTED {
		return errors.New("nats publisher connection is not established")
	}
	if _, err := p.js.Stream(ctx, p.stream); err != nil {
		return fmt.Errorf("look up stream %q: %w", p.stream, err)
	}
	return nil
}

func (p *Publisher) Close() error {
	if p.conn != nil {
		p.conn.Close()
	}
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
