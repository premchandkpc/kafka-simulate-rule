package ports

import (
	"context"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
)

// BrokerConsumer defines the interface for consuming messages from a message broker
type BrokerConsumer interface {
	Fetch(ctx context.Context, maxMessages int) ([]Delivery, error)
	Subscribe(ctx context.Context, subjects []string) error
	Unsubscribe(ctx context.Context) error
	Close() error
}

// Delivery represents a message delivery from the broker
type Delivery interface {
	Event() (*domain.EventEnvelope, error)
	Ack(ctx context.Context) error
	Nak(ctx context.Context) error
	Retry(ctx context.Context, delay time.Duration) error
	Raw() []byte
	Headers() map[string]string
	Subject() string
}

// BrokerPublisher defines the interface for publishing messages to a broker
type BrokerPublisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
	PublishBatch(ctx context.Context, messages []PublishMessage) error
	PublishToShard(ctx context.Context, baseSubject string, shard uint32, data []byte) error
	Close() error
}

type PublishMessage struct {
	Subject string
	Data    []byte
	Headers map[string]string
}

// StreamAdmin provides administrative operations for streams
type StreamAdmin interface {
	CreateStream(ctx context.Context, config StreamConfig) error
	DeleteStream(ctx context.Context, name string) error
	CreateConsumer(ctx context.Context, stream, consumer string, config ConsumerConfig) error
	DeleteConsumer(ctx context.Context, stream, consumer string) error
	GetStreamInfo(ctx context.Context, name string) (*StreamInfo, error)
}

type PlacementConfig struct {
	Tags map[string]string
}

type StreamConfig struct {
	Name        string
	Subjects    []string
	Retention   RetentionPolicy
	Storage     StorageType
	MaxMsgs     int64
	MaxBytes    int64
	MaxAge      time.Duration
	Replicas    int
	Placement   *PlacementConfig
}

type ConsumerConfig struct {
	Durable       string
	AckPolicy     AckPolicy
	DeliverPolicy DeliverPolicy
	FilterSubject string
	MaxDeliver    int
	AckWait       time.Duration
	MaxAckPending int
	NumReplicas   int
}

type StreamInfo struct {
	Name      string
	Subjects  []string
	State     StreamState
	Msgs      uint64
	Bytes     uint64
	Consumers []ConsumerInfo
}

type ConsumerInfo struct {
	Name         string
	NumPending   uint64
	NumAckPending uint64
	Delivered    uint64
	AckFloor     uint64
}

type RetentionPolicy string

const (
	RetentionPolicyLimits   RetentionPolicy = "limits"
	RetentionPolicyInterest RetentionPolicy = "interest"
	RetentionPolicyWorkQueue RetentionPolicy = "workqueue"
)

type StorageType string

const (
	StorageTypeFile   StorageType = "file"
	StorageTypeMemory StorageType = "memory"
)

type AckPolicy string

const (
	AckPolicyExplicit    AckPolicy = "explicit"
	AckPolicyAll         AckPolicy = "all"
	AckPolicyNone        AckPolicy = "none"
)

type DeliverPolicy string

const (
	DeliverPolicyAll       DeliverPolicy = "all"
	DeliverPolicyLast      DeliverPolicy = "last"
	DeliverPolicyNew       DeliverPolicy = "new"
	DeliverPolicyByStartSeq DeliverPolicy = "by_start_sequence"
	DeliverPolicyByStartTime DeliverPolicy = "by_start_time"
)

type StreamState string

const (
	StreamStateActive   StreamState = "active"
	StreamStateDeleted  StreamState = "deleted"
)