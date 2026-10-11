package keyqueue

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type KeyQueue struct {
	mu          sync.Mutex
	queues      map[string]*keyQueue
	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
	stopOnce    sync.Once
	submitWG    sync.WaitGroup
	maxPerKey   int
	globalSem   chan struct{}
	workerCount int
	stopWorkers   chan struct{}
	stopSubmitting chan struct{}
	wg            sync.WaitGroup
	processor   func(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error
	clock       func() time.Time

	// Hot-key detection
	hotKeyThreshold     int
	hotKeyCheckInterval time.Duration
	hotKeyCallback      func(partitionKey string, depth int)
	muHot               sync.Mutex
	hotKeys             map[string]time.Time // key -> first detected time
	stopHotKeyCheck     chan struct{}
	wgHot               sync.WaitGroup
}

type keyQueue struct {
	ch           chan *queuedItem
	partitionKey string
	processing   bool
	submitters   int
	mu           sync.Mutex
}

type queuedItem struct {
	env          *domain.EventEnvelope
	delivery     ports.Delivery
	fencingToken int64
	vshard       uint32
	workerID     string
}

func NewKeyQueue(
	maxGlobalInFlight int,
	maxPerKeyQueue int,
	workerCount int,
	processor func(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error,
	clock func() time.Time,
	hotKeyThreshold int,
	hotKeyCheckInterval time.Duration,
	hotKeyCallback func(partitionKey string, depth int),
) *KeyQueue {
	if maxGlobalInFlight <= 0 {
		maxGlobalInFlight = 100
	}
	if maxPerKeyQueue <= 0 {
		maxPerKeyQueue = 100
	}
	if workerCount <= 0 {
		workerCount = 1
	}
	if clock == nil {
		clock = time.Now
	}
	if hotKeyThreshold <= 0 {
		hotKeyThreshold = 1000
	}
	if hotKeyCheckInterval <= 0 {
		hotKeyCheckInterval = 10 * time.Second
	}
	return &KeyQueue{
		queues:              make(map[string]*keyQueue),
		maxPerKey:           maxPerKeyQueue,
		globalSem:           make(chan struct{}, maxGlobalInFlight),
		workerCount:         workerCount,
		stopWorkers:         make(chan struct{}),
		stopSubmitting:      make(chan struct{}),
		processor:           processor,
		clock:               clock,
		hotKeyThreshold:     hotKeyThreshold,
		hotKeyCheckInterval: hotKeyCheckInterval,
		hotKeyCallback:      hotKeyCallback,
		hotKeys:             make(map[string]time.Time),
		stopHotKeyCheck:     make(chan struct{}),
	}
}

// Start launches the queue workers once. A stopped KeyQueue cannot be restarted.
func (kq *KeyQueue) Start(ctx context.Context) {
	if ctx == nil {
		return
	}
	kq.lifecycleMu.Lock()
	if kq.started || kq.stopped {
		kq.lifecycleMu.Unlock()
		return
	}
	kq.started = true
	for i := 0; i < kq.workerCount; i++ {
		kq.wg.Add(1)
		go kq.workerLoop(ctx, i)
	}
	if kq.hotKeyCallback != nil {
		kq.wgHot.Add(1)
		go kq.hotKeyCheckLoop(ctx)
	}
	kq.lifecycleMu.Unlock()
}

// Stop asks all queue goroutines to stop and waits for them. It is safe to call
// Stop more than once.
func (kq *KeyQueue) Stop() {
	kq.stopOnce.Do(func() {
		kq.lifecycleMu.Lock()
		kq.stopped = true
		close(kq.stopSubmitting)
		kq.lifecycleMu.Unlock()

		// Let submissions already admitted finish before closing the worker
		// signal channels. Workers remain alive while these submissions drain.
		kq.submitWG.Wait()
		close(kq.stopWorkers)
		close(kq.stopHotKeyCheck)
	})
	kq.wg.Wait()
	kq.wgHot.Wait()
}

func (kq *KeyQueue) hotKeyCheckLoop(ctx context.Context) {
	defer kq.wgHot.Done()
	ticker := time.NewTicker(kq.hotKeyCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-kq.stopHotKeyCheck:
			return
		case <-ticker.C:
			kq.checkHotKeys()
		}
	}
}

func (kq *KeyQueue) checkHotKeys() {
	kq.mu.Lock()
	queues := make(map[string]*keyQueue, len(kq.queues))
	for k, v := range kq.queues {
		queues[k] = v
	}
	kq.mu.Unlock()

	now := kq.clock()
	type hotKeyNotice struct {
		key   string
		depth int
	}
	var notices []hotKeyNotice

	kq.muHot.Lock()
	for key, q := range queues {
		depth := len(q.ch)
		if depth >= kq.hotKeyThreshold {
			if _, tracked := kq.hotKeys[key]; !tracked {
				kq.hotKeys[key] = now
				notices = append(notices, hotKeyNotice{key: key, depth: depth})
			}
		} else {
			delete(kq.hotKeys, key)
		}
	}
	// Queues may have been removed after draining. Remove their hot-key state too.
	for key := range kq.hotKeys {
		if _, exists := queues[key]; !exists {
			delete(kq.hotKeys, key)
		}
	}
	kq.muHot.Unlock()

	// Run callbacks on the owned checker goroutine instead of leaking one goroutine
	// per hot key. Callback implementations should be quick and non-blocking.
	for _, notice := range notices {
		kq.hotKeyCallback(notice.key, notice.depth)
	}
}

func (kq *KeyQueue) Submit(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error {
	if ctx == nil {
		return errors.New("submit event: context is nil")
	}
	if env == nil {
		return errors.New("submit event: envelope is nil")
	}

	kq.lifecycleMu.Lock()
	if !kq.started || kq.stopped {
		kq.lifecycleMu.Unlock()
		return errors.New("submit event: key queue is not running")
	}
	// Add while holding lifecycleMu so Stop cannot race Wait with a new Add.
	kq.submitWG.Add(1)
	kq.lifecycleMu.Unlock()
	defer kq.submitWG.Done()

	select {
	case kq.globalSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-kq.stopSubmitting:
		return errors.New("submit event: key queue is stopping")
	}

	kq.mu.Lock()
	q, ok := kq.queues[env.PartitionKey]
	if !ok {
		q = &keyQueue{
			ch:           make(chan *queuedItem, kq.maxPerKey),
			partitionKey: env.PartitionKey,
		}
		kq.queues[env.PartitionKey] = q
	}
	q.mu.Lock()
	q.submitters++
	q.mu.Unlock()
	kq.mu.Unlock()

	select {
	case q.ch <- &queuedItem{
		env:          env,
		delivery:     delivery,
		fencingToken: fencingToken,
		vshard:       vshard,
		workerID:     workerID,
	}:
		kq.finishSubmit(q)
		return nil
	case <-ctx.Done():
		<-kq.globalSem
		kq.finishSubmit(q)
		return ctx.Err()
	case <-kq.stopSubmitting:
		<-kq.globalSem
		kq.finishSubmit(q)
		return errors.New("submit event: key queue is stopping")
	}
}

func (kq *KeyQueue) finishSubmit(q *keyQueue) {
	kq.mu.Lock()
	q.mu.Lock()
	q.submitters--
	if q.submitters < 0 {
		q.submitters = 0
	}
	if kq.queues[q.partitionKey] == q && !q.processing && q.submitters == 0 && len(q.ch) == 0 {
		delete(kq.queues, q.partitionKey)
	}
	q.mu.Unlock()
	kq.mu.Unlock()
}

func (kq *KeyQueue) workerLoop(ctx context.Context, workerID int) {
	defer kq.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-kq.stopWorkers:
			return
		default:
			item := kq.nextEvent()
			if item == nil {
				timer := time.NewTimer(10 * time.Millisecond)
				select {
				case <-ctx.Done():
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					return
				case <-kq.stopWorkers:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					return
				case <-timer.C:
				}
				continue
			}

			if err := kq.processor(ctx, item.env, item.delivery, item.fencingToken, item.vshard, item.workerID); err != nil {
				log.Printf("keyqueue worker %d: process error for key %s: %v", workerID, item.env.PartitionKey, err)
			}
			kq.finishEvent(item.env.PartitionKey)
			<-kq.globalSem
		}
	}
}

func (kq *KeyQueue) nextEvent() *queuedItem {
	kq.mu.Lock()
	var selected *keyQueue
	for _, q := range kq.queues {
		q.mu.Lock()
		if !q.processing && len(q.ch) > 0 {
			selected = q
			q.processing = true
		}
		q.mu.Unlock()
		if selected != nil {
			break
		}
	}
	kq.mu.Unlock()

	if selected == nil {
		return nil
	}

	item := <-selected.ch
	// Keep selected.processing true until the processor has completed. Otherwise
	// another worker can dequeue the next event for this key concurrently.
	return item
}

func (kq *KeyQueue) finishEvent(partitionKey string) {
	kq.mu.Lock()
	q := kq.queues[partitionKey]
	if q == nil {
		kq.mu.Unlock()
		return
	}
	q.mu.Lock()
	q.processing = false
	if q.submitters == 0 && len(q.ch) == 0 {
		delete(kq.queues, partitionKey)
	}
	q.mu.Unlock()
	kq.mu.Unlock()
}

func (kq *KeyQueue) Stats() map[string]int {
	kq.mu.Lock()
	defer kq.mu.Unlock()
	stats := make(map[string]int, len(kq.queues))
	for k, q := range kq.queues {
		stats[k] = len(q.ch)
	}
	return stats
}

func (kq *KeyQueue) QueueDepth(partitionKey string) int {
	kq.mu.Lock()
	defer kq.mu.Unlock()
	if q, ok := kq.queues[partitionKey]; ok {
		return len(q.ch)
	}
	return 0
}

func (kq *KeyQueue) ActiveKeys() int {
	kq.mu.Lock()
	defer kq.mu.Unlock()
	return len(kq.queues)
}
