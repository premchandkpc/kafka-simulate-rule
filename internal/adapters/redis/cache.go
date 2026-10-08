package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/flowrule/flowrule/internal/ports"
)

type Cache struct {
	client *redis.Client
}

func NewCache(client *Client) *Cache {
	return &Cache{client: client.client}
}

func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	val, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("cache get: %w", err)
	}
	return val, nil
}

func (c *Cache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	err := c.client.Set(ctx, key, value, ttl).Err()
	if err != nil {
		return fmt.Errorf("cache set: %w", err)
	}
	return nil
}

func (c *Cache) Delete(ctx context.Context, key string) error {
	err := c.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("cache delete: %w", err)
	}
	return nil
}

func (c *Cache) Exists(ctx context.Context, key string) (bool, error) {
	n, err := c.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("cache exists: %w", err)
	}
	return n > 0, nil
}

func (c *Cache) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	val, err := c.client.IncrBy(ctx, key, delta).Result()
	if err != nil {
		return 0, fmt.Errorf("cache increment: %w", err)
	}
	return val, nil
}

func (c *Cache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	err := c.client.Expire(ctx, key, ttl).Err()
	if err != nil {
		return fmt.Errorf("cache expire: %w", err)
	}
	return nil
}

func (c *Cache) TTL(ctx context.Context, key string) (time.Duration, error) {
	dur, err := c.client.TTL(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("cache ttl: %w", err)
	}
	return dur, nil
}

func (c *Cache) Keys(ctx context.Context, pattern string) ([]string, error) {
	keys, err := c.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("cache keys: %w", err)
	}
	return keys, nil
}

func (c *Cache) Flush(ctx context.Context) error {
	err := c.client.FlushDB(ctx).Err()
	if err != nil {
		return fmt.Errorf("cache flush: %w", err)
	}
	return nil
}

type Lock struct {
	client     *redis.Client
	key        string
	token      string
	ttl        time.Duration
	watchCtx   context.Context
	watchCancel context.CancelFunc
}

func (c *Cache) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	key = "lock:" + key
	token := fmt.Sprintf("%d", time.Now().UnixNano())
	
	ok, err := c.client.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("lock acquire: %w", err)
	}
	return ok, nil
}

func (c *Cache) Release(ctx context.Context, key, token string) (bool, error) {
	key = "lock:" + key
	
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	
	result, err := script.Run(ctx, c.client, []string{key}, token).Int()
	if err != nil {
		return false, fmt.Errorf("lock release: %w", err)
	}
	return result == 1, nil
}

func (c *Cache) Extend(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	key = "lock:" + key
	
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("expire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)
	
	result, err := script.Run(ctx, c.client, []string{key}, token, int(ttl.Seconds())).Int()
	if err != nil {
		return false, fmt.Errorf("lock extend: %w", err)
	}
	return result == 1, nil
}

func (c *Cache) Lock(ctx context.Context, key string, ttl time.Duration) (ports.LockGuard, error) {
	lock := &Lock{
		client:     c.client,
		key:        "lock:" + key,
		token:      fmt.Sprintf("%d", time.Now().UnixNano()),
		ttl:        ttl,
	}
	
	ok, err := lock.client.SetNX(ctx, lock.key, lock.token, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("lock acquire: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("lock not acquired")
	}
	
	lock.watchCtx, lock.watchCancel = context.WithCancel(ctx)
	go lock.renewLoop()
	
	return lock, nil
}

func (l *Lock) renewLoop() {
	ticker := time.NewTicker(l.ttl / 3)
	defer ticker.Stop()
	
	for {
		select {
		case <-l.watchCtx.Done():
			return
		case <-ticker.C:
			script := redis.NewScript(`
				if redis.call("get", KEYS[1]) == ARGV[1] then
					return redis.call("expire", KEYS[1], ARGV[2])
				else
					return 0
				end
			`)
			script.Run(l.watchCtx, l.client, []string{l.key}, l.token, int(l.ttl.Seconds()))
		}
	}
}

func (l *Lock) Key() string {
	return l.key
}

func (l *Lock) Token() string {
	return l.token
}

func (l *Lock) Release(ctx context.Context) error {
	if l.watchCancel != nil {
		l.watchCancel()
	}
	
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	
	_, err := script.Run(ctx, l.client, []string{l.key}, l.token).Int()
	return err
}

func (l *Lock) Extend(ctx context.Context, ttl time.Duration) error {
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("expire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)
	
	_, err := script.Run(ctx, l.client, []string{l.key}, l.token, int(ttl.Seconds())).Int()
	return err
}

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

func (r *RateLimiter) Limit(ctx context.Context, key string) (int, error) {
	key = "ratelimit:" + key
	count, err := r.client.ZCard(ctx, key).Result()
	return int(count), err
}

func (r *RateLimiter) Remaining(ctx context.Context, key string) (int, error) {
	return 0, nil
}

func (r *RateLimiter) Reset(ctx context.Context, key string) error {
	key = "ratelimit:" + key
	return r.client.Del(ctx, key).Err()
}