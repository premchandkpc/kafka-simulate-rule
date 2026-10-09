package ports

import (
	"context"
	"time"
)

// Cache provides a generic key-value cache interface
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
	Increment(ctx context.Context, key string, delta int64) (int64, error)
	Expire(ctx context.Context, key string, ttl time.Duration) error
	TTL(ctx context.Context, key string) (time.Duration, error)
	Keys(ctx context.Context, pattern string) ([]string, error)
	Flush(ctx context.Context) error
}

// DistributedLock provides distributed locking primitives
type DistributedLock interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Release(ctx context.Context, key, token string) (bool, error)
	Extend(ctx context.Context, key, token string, ttl time.Duration) (bool, error)
	Lock(ctx context.Context, key string, ttl time.Duration) (LockGuard, error)
}

type LockGuard interface {
	Key() string
	Token() string
	Release(ctx context.Context) error
	Extend(ctx context.Context, ttl time.Duration) error
}

// RateLimiter provides rate limiting primitives
type RateLimiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error)
	Limit(ctx context.Context, key string) (int, error)
	Remaining(ctx context.Context, key string) (int, error)
	Reset(ctx context.Context, key string) error
}

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

// SessionStore provides session management
type SessionStore interface {
	Create(ctx context.Context, session *Session) error
	Get(ctx context.Context, id string) (*Session, error)
	Update(ctx context.Context, session *Session) error
	Delete(ctx context.Context, id string) error
	DeleteExpired(ctx context.Context) (int64, error)
}

type Session struct {
	ID        string
	UserID    string
	Data      map[string]interface{}
	CreatedAt time.Time
	ExpiresAt time.Time
}

// HealthChecker provides health check primitives
type HealthChecker interface {
	Check(ctx context.Context) HealthStatus
	Name() string
}

type HealthStatus struct {
	Healthy   bool
	Message   string
	Details   map[string]interface{}
	Timestamp time.Time
}
