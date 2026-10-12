package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowrule/flowrule/internal/ports"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const minimumLockTTL = time.Second

var (
	ErrLockNotAcquired = errors.New("redis lock not acquired")
	ErrLockLost        = errors.New("redis lock ownership lost")
)

// LockManager owns distributed-lock creation independently of the key/value cache.
type LockManager struct {
	client *redis.Client
}

func NewLockManager(client *Client) *LockManager {
	return &LockManager{client: client.client}
}

type Lock struct {
	client      *redis.Client
	key         string
	redisKey    string
	token       string
	ttl         time.Duration
	watchCtx    context.Context
	watchCancel context.CancelFunc
	done        chan struct{}
	lost        chan struct{}
	lostOnce    sync.Once
	stopping    atomic.Bool
}

func (m *LockManager) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	_, err := m.Lock(ctx, key, ttl)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (m *LockManager) Extend(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return false, fmt.Errorf("redis lock extend: key is required")
	}
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)
	result, err := script.Run(ctx, m.client, []string{"lock:" + key}, token, ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("extend redis lock: %w", err)
	}
	return result == 1, nil
}

func (m *LockManager) Release(ctx context.Context, key, token string) (bool, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return false, fmt.Errorf("redis lock release: key is required")
	}
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	result, err := script.Run(ctx, m.client, []string{"lock:" + key}, token).Int()
	if err != nil {
		return false, fmt.Errorf("release redis lock: %w", err)
	}
	return result == 1, nil
}

func (m *LockManager) Lock(ctx context.Context, key string, ttl time.Duration) (ports.LockGuard, error) {
	if ctx == nil {
		return nil, fmt.Errorf("redis lock: context is required")
	}
	if m == nil {
		return nil, fmt.Errorf("redis lock: manager is required")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, fmt.Errorf("redis lock: key is required")
	}
	if ttl < minimumLockTTL {
		return nil, fmt.Errorf("redis lock: ttl must be at least %s", minimumLockTTL)
	}
	if m.client == nil {
		return nil, fmt.Errorf("redis lock: client is required")
	}

	token := uuid.NewString()
	redisKey := "lock:" + key
	acquired, err := m.client.SetNX(ctx, redisKey, token, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("acquire redis lock: %w", err)
	}
	if !acquired {
		return nil, ErrLockNotAcquired
	}

	watchCtx, watchCancel := context.WithCancel(ctx)
	lock := &Lock{
		client: m.client, key: key, redisKey: redisKey, token: token, ttl: ttl,
		watchCtx: watchCtx, watchCancel: watchCancel, done: make(chan struct{}), lost: make(chan struct{}),
	}
	go lock.renewLoop()
	return lock, nil
}

func (l *Lock) markLost() {
	l.lostOnce.Do(func() {
		close(l.lost)
		if l.watchCancel != nil {
			l.watchCancel()
		}
	})
}

func (l *Lock) isLost() bool {
	select {
	case <-l.lost:
		return true
	default:
		return false
	}
}

func (l *Lock) renewLoop() {
	defer close(l.done)
	ticker := time.NewTicker(l.ttl / 3)
	defer ticker.Stop()

	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)
	for {
		select {
		case <-l.watchCtx.Done():
			if !l.stopping.Load() {
				l.markLost()
			}
			return
		case <-ticker.C:
			result, err := script.Run(l.watchCtx, l.client, []string{l.redisKey}, l.token, l.ttl.Milliseconds()).Int()
			if err != nil || result != 1 {
				l.markLost()
				return
			}
		}
	}
}

func (l *Lock) Key() string {
	return l.key
}

func (l *Lock) Token() string {
	return l.token
}

func (l *Lock) Lost() <-chan struct{} {
	return l.lost
}

func (l *Lock) Release(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("release redis lock: context is required")
	}
	l.stopping.Store(true)
	if l.watchCancel != nil {
		l.watchCancel()
	}
	<-l.done

	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	result, err := script.Run(ctx, l.client, []string{l.redisKey}, l.token).Int()
	if err != nil {
		return fmt.Errorf("release redis lock: %w", err)
	}
	if result != 1 {
		l.markLost()
		return ErrLockLost
	}
	return nil
}

func (l *Lock) Extend(ctx context.Context, ttl time.Duration) error {
	if ctx == nil {
		return fmt.Errorf("extend redis lock: context is required")
	}
	if ttl < l.ttl {
		return fmt.Errorf("redis lock: extension ttl must be at least the original ttl %s", l.ttl)
	}
	if l.isLost() {
		return ErrLockLost
	}
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)
	result, err := script.Run(ctx, l.client, []string{l.redisKey}, l.token, ttl.Milliseconds()).Int()
	if err != nil {
		return fmt.Errorf("extend redis lock: %w", err)
	}
	if result != 1 {
		l.markLost()
		return ErrLockLost
	}
	return nil
}

var _ ports.DistributedLock = (*LockManager)(nil)
var _ ports.LockGuard = (*Lock)(nil)
