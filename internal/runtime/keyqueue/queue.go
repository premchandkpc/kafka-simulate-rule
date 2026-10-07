package keyqueue

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type KeyQueue struct {
	mu            sync.Mutex
	queues        map[string]*keyQueue
	maxPerKey     int
	globalSem     chan struct{}
	workerCount   int
	stopWorkers   chan struct{}
	wg            sync.WaitGroup
	processor     func(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error
	clock         func() time.Time
}

type keyQueue struct {
	ch           chan *queuedItem
	partitionKey string
	processing   bool
	mu           sync.Mutex
}

type queuedItem struct {
	env       *domain.EventEnvelope
	delivery  ports.Delivery
	fencingToken int64
	vshard    uint32
	workerID  string
}

func NewKeyQueue(
	maxGlobalInFlight int,
	maxPerKeyQueue int,
	workerCount int,
	processor func(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error,
	clock func() time.Time,
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
	return &KeyQueue{
		queues:       make(map[string]*keyQueue),
		maxPerKey:    maxPerKeyQueue,
		globalSem:    make(chan struct{}, maxGlobalInFlight),
		workerCount:  workerCount,
		stopWorkers:  make(chan struct{}),
		processor:    processor,
		clock:        clock,
	}
}

func (kq *KeyQueue) Start(ctx context.Context) {
	for i := 0; i < kq.workerCount; i++ {
		kq.wg.Add(1)
		go kq.workerLoop(ctx, i)
	}
}

func (kq *KeyQueue) Stop() {
	close(kq.stopWorkers)
	kq.wg.Wait()
}

func (kq *KeyQueue) Submit(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error {
	select {
	case kq.globalSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
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
	kq.mu.Unlock()

	select {
	case q.ch <- &queuedItem{
		env:          env,
		delivery:     delivery,
		fencingToken: fencingToken,
		vshard:       vshard,
		workerID:     workerID,
	}:
		return nil
	case <-ctx.Done():
		<-kq.globalSem
		return ctx.Err()
	}
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
				time.Sleep(10 * time.Millisecond)
				continue
			}

			if err := kq.processor(ctx, item.env, item.delivery, item.fencingToken, item.vshard, item.workerID); err != nil {
				log.Printf("keyqueue worker %d: process error for key %s: %v", workerID, item.env.PartitionKey, err)
			}
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
	selected.mu.Lock()
	selected.processing = false
	selected.mu.Unlock()
	return item
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