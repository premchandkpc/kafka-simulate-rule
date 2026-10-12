package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/flowrule/flowrule/internal/ports"
	"github.com/redis/go-redis/v9"
)

type LockManager struct {
	client *redis.Client
}

func NewLockManager(client *Client) *LockManager {
	return &LockManager{client: client.client}
}

type Lock struct {
	client      *redis.Client
	key         string
	token       string
	ttl         time.Duration
	watchCtx    context.Context
	watchCancel context.CancelFunc
}

func (m *LockManager) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	key = "lock:" + key
	token := fmt.Sprintf("%d", time.Now().UnixNano())

	ok, err := m.client.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("lock acquire: %w", err)
	}
	return ok, nil
}

func (m *LockManager) Release(ctx context.Context, key, token string) (bool, error) {
	key = "lock:" + key

	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)

	result, err := script.Run(ctx, m.client, []string{key}, token).Int()
	if err != nil {
		return false, fmt.Errorf("lock release: %w", err)
	}
	return result == 1, nil
}

func (m *LockManager) Extend(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	key = "lock:" + key

	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("expire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)

	result, err := script.Run(ctx, m.client, []string{key}, token, int(ttl.Seconds())).Int()
	if err != nil {
		return false, fmt.Errorf("lock extend: %w", err)
	}
	return result == 1, nil
}

func (m *LockManager) Lock(ctx context.Context, key string, ttl time.Duration) (ports.LockGuard, error) {
	lock := &Lock{
		client: m.client,
		key:    "lock:" + key,
		token:  fmt.Sprintf("%d", time.Now().UnixNano()),
		ttl:    ttl,
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



var _ ports.DistributedLock = (*LockManager)(nil)
var _ ports.LockGuard = (*Lock)(nil)
