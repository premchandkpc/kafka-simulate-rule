package flowrule

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type natsTransport struct {
	cfg    *TransportConfig
	conn   *nats.Conn
	js     jetstream.JetStream
	mu     sync.RWMutex
	closed bool
}

func newNATSTransport(cfg *TransportConfig) (Transport, error) {
	conn, err := nats.Connect(cfg.Config.NATSURL,
		nats.Timeout(10*time.Second),
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		return nil, fmt.Errorf("connect NATS: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create JetStream: %w", err)
	}

	// Ensure stream exists
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subjects := make([]string, 0, len(cfg.Config.NATSSubjects)*int(cfg.Config.NumShards))
	for _, s := range cfg.Config.NATSSubjects {
		for shard := uint32(0); shard < cfg.Config.NumShards; shard++ {
			subjects = append(subjects, fmt.Sprintf("%s.shard.%d", s, shard))
		}
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      cfg.Config.NATSStream,
		Subjects:  subjects,
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
		MaxMsgs:   -1,
		Discard:   jetstream.DiscardOld,
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create stream: %w", err)
	}

	return &natsTransport{
		cfg:  cfg,
		conn: conn,
		js:   js,
	}, nil
}

func (t *natsTransport) Send(ctx context.Context, env *Envelope) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return ErrTransportClosed
	}

	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	// Compute shard
	shard := computeShard(env.TenantID, env.PartitionKey, t.cfg.Config.NumShards)

	// Use first subject as base
	baseSubject := t.cfg.Config.NATSSubjects[0]
	subject := fmt.Sprintf("%s.shard.%d", baseSubject, shard)

	// Create context with timeout
	sendCtx, cancel := context.WithTimeout(ctx, t.cfg.Config.SendTimeout)
	defer cancel()

	_, err = t.js.Publish(sendCtx, subject, data)
	if err != nil {
		return fmt.Errorf("publish to NATS: %w", err)
	}

	return nil
}

func (t *natsTransport) SendBatch(ctx context.Context, envs []*Envelope) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return ErrTransportClosed
	}

	if len(envs) == 0 {
		return nil
	}

	sendCtx, cancel := context.WithTimeout(ctx, t.cfg.Config.SendTimeout)
	defer cancel()

	for _, env := range envs {
		data, err := json.Marshal(env)
		if err != nil {
			return fmt.Errorf("marshal envelope: %w", err)
		}

		shard := computeShard(env.TenantID, env.PartitionKey, t.cfg.Config.NumShards)
		baseSubject := t.cfg.Config.NATSSubjects[0]
		subject := fmt.Sprintf("%s.shard.%d", baseSubject, shard)

		_, err = t.js.Publish(sendCtx, subject, data)
		if err != nil {
			return fmt.Errorf("publish to NATS: %w", err)
		}
	}

	return nil
}

func (t *natsTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}
	t.closed = true

	if t.conn != nil {
		t.conn.Close()
	}
	return nil
}

func (t *natsTransport) Name() string {
	return "nats"
}

// ErrTransportClosed is returned when sending on a closed transport
var ErrTransportClosed = fmt.Errorf("transport closed")