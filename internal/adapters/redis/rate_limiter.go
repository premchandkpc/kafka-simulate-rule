package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/flowrule/flowrule/internal/ports"
	"github.com/redis/go-redis/v9"
)

type RateLimiter struct {
	client *redis.Client
}

func NewRateLimiter(client *Client) *RateLimiter {
	return &RateLimiter{client: client.client}
}

func (r *RateLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	key = "ratelimit:" + key
	now := time.Now().UnixMilli()
	windowStart := now - window.Milliseconds()

	pipe := r.client.TxPipeline()
	pipe.ZRemRangeByScore(ctx, key, "0", fmt.Sprintf("%d", windowStart))
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(now), Member: fmt.Sprintf("%d", now)})
	pipe.ZCard(ctx, key)
	pipe.Expire(ctx, key, window)

	results, err := pipe.Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("rate limit check: %w", err)
	}

	count := results[2].(*redis.IntCmd).Val()
	return count <= int64(limit), nil
}

func (r *RateLimiter) Reset(ctx context.Context, key string) error {
	key = "ratelimit:" + key
	return r.client.Del(ctx, key).Err()
}

var _ ports.RateLimiter = (*RateLimiter)(nil)
