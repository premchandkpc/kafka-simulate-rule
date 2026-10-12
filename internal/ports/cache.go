package ports

import (
	"context"
	"time"
)

// Cache is a small key/value capability for consumers that explicitly need
// caching. Key enumeration and whole-database flush are deliberately excluded.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
	Increment(ctx context.Context, key string, delta int64) (int64, error)
	Expire(ctx context.Context, key string, ttl time.Duration) error
	TTL(ctx context.Context, key string) (time.Duration, error)
}

// DistributedLock exposes only acquisition; token management belongs to the
// returned guard rather than being split across unrelated primitive calls.
type DistributedLock interface {
	Lock(ctx context.Context, key string, ttl time.Duration) (LockGuard, error)
}

// LockGuard represents one lock lease held by the caller.
type LockGuard interface {
	Key() string
	Token() string
	Lost() <-chan struct{}
	Release(ctx context.Context) error
	Extend(ctx context.Context, ttl time.Duration) error
}

// RateLimiter provides operations with unambiguous semantics. A "remaining"
// count is not meaningful unless the caller also supplies the configured limit.
type RateLimiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error)
	Reset(ctx context.Context, key string) error
}
