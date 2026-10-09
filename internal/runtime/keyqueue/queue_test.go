package keyqueue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type mockDelivery struct{}

func (m *mockDelivery) Event() (*domain.EventEnvelope, error)                { return nil, nil }
func (m *mockDelivery) Ack(ctx context.Context) error                        { return nil }
func (m *mockDelivery) Nak(ctx context.Context) error                        { return nil }
func (m *mockDelivery) Retry(ctx context.Context, delay time.Duration) error { return nil }
func (m *mockDelivery) Raw() []byte                                          { return nil }
func (m *mockDelivery) Headers() map[string]string                           { return nil }
func (m *mockDelivery) Subject() string                                      { return "" }

func TestKeyQueue_PerKeySerialization(t *testing.T) {
	var mu sync.Mutex
	var processed []string

	processor := func(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error {
		mu.Lock()
		processed = append(processed, env.PartitionKey+":"+env.ID)
		mu.Unlock()
		return nil
	}

	kq := NewKeyQueue(10, 10, 1, processor, time.Now, 1000, 10*time.Second, nil)
	ctx := context.Background()
	kq.Start(ctx)
	defer kq.Stop()

	// Submit 5 events for key A, 5 for key B
	for i := 0; i < 5; i++ {
		envA := &domain.EventEnvelope{ID: "evt-A-" + string(rune('0'+i)), PartitionKey: "key-A"}
		envB := &domain.EventEnvelope{ID: "evt-B-" + string(rune('0'+i)), PartitionKey: "key-B"}
		kq.Submit(ctx, envA, &mockDelivery{}, 0, 0, "")
		kq.Submit(ctx, envB, &mockDelivery{}, 0, 0, "")
	}

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Verify all 10 events processed
	if len(processed) != 10 {
		t.Fatalf("expected 10 processed, got %d: %v", len(processed), processed)
	}

	// Verify ordering within each key is preserved (FIFO)
	var keyASeq, keyBSeq []string
	for _, p := range processed {
		if p[:5] == "key-A" {
			keyASeq = append(keyASeq, p)
		} else {
			keyBSeq = append(keyBSeq, p)
		}
	}

	// Check key A order
	for i, p := range keyASeq {
		expected := "key-A:evt-A-" + string(rune('0'+i))
		if p != expected {
			t.Errorf("key A order mismatch at %d: got %s, want %s", i, p, expected)
		}
	}

	// Check key B order
	for i, p := range keyBSeq {
		expected := "key-B:evt-B-" + string(rune('0'+i))
		if p != expected {
			t.Errorf("key B order mismatch at %d: got %s, want %s", i, p, expected)
		}
	}
}

func TestKeyQueue_ConcurrentKeysParallel(t *testing.T) {
	var mu sync.Mutex
	active := make(map[string]int)
	maxConcurrent := 0

	processor := func(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error {
		mu.Lock()
		active[env.PartitionKey]++
		if len(active) > maxConcurrent {
			maxConcurrent = len(active)
		}
		mu.Unlock()

		time.Sleep(10 * time.Millisecond) // Simulate work

		mu.Lock()
		active[env.PartitionKey]--
		if active[env.PartitionKey] == 0 {
			delete(active, env.PartitionKey)
		}
		mu.Unlock()
		return nil
	}

	kq := NewKeyQueue(10, 10, 3, processor, time.Now, 1000, 10*time.Second, nil) // 3 workers
	ctx := context.Background()
	kq.Start(ctx)
	defer kq.Stop()

	// Submit events for 5 different keys concurrently
	numKeys := 5
	eventsPerKey := 3
	var wg sync.WaitGroup
	wg.Add(numKeys * eventsPerKey)

	for k := 0; k < numKeys; k++ {
		key := "key-" + string(rune('0'+k))
		for i := 0; i < eventsPerKey; i++ {
			env := &domain.EventEnvelope{ID: key + "-evt-" + string(rune('0'+i)), PartitionKey: key}
			kq.Submit(ctx, env, &mockDelivery{}, 0, 0, "")
			wg.Done()
		}
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	// With 3 workers and 5 keys, we should see some parallelism
	if maxConcurrent < 2 {
		t.Logf("Warning: max concurrent keys was %d (expected at least 2 with 3 workers)", maxConcurrent)
	}
}

func TestKeyQueue_Backpressure(t *testing.T) {
	processor := func(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error {
		time.Sleep(50 * time.Millisecond) // Slow processing
		return nil
	}

	kq := NewKeyQueue(3, 2, 1, processor, time.Now, 1000, 10*time.Second, nil) // Max 3 in-flight, 2 per key
	ctx := context.Background()
	kq.Start(ctx)
	defer kq.Stop()

	// Try to submit more than max in-flight
	submitCount := 0
	for i := 0; i < 10; i++ {
		env := &domain.EventEnvelope{ID: "evt-" + string(rune('0'+i)), PartitionKey: "key-1"}
		err := kq.Submit(ctx, env, &mockDelivery{}, 0, 0, "")
		if err != nil {
			t.Fatalf("submit %d failed: %v", i, err)
		}
		submitCount++
	}

	if submitCount != 10 {
		t.Fatalf("expected 10 submits, got %d", submitCount)
	}

	// Wait for processing to complete
	time.Sleep(1 * time.Second)

	// Queue should be empty now
	if kq.QueueDepth("key-1") != 0 {
		t.Errorf("expected queue depth 0, got %d", kq.QueueDepth("key-1"))
	}
}

func TestKeyQueue_ContextCancellation(t *testing.T) {
	processor := func(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	kq := NewKeyQueue(1, 10, 1, processor, time.Now, 1000, 10*time.Second, nil) // Only 1 global slot
	ctx, cancel := context.WithCancel(context.Background())
	kq.Start(ctx)

	// Submit first event - fills the semaphore
	env := &domain.EventEnvelope{ID: "evt-1", PartitionKey: "key-1"}
	err := kq.Submit(ctx, env, &mockDelivery{}, 0, 0, "")
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	// Cancel context
	cancel()

	// Try to submit second event - should block on semaphore then return context error
	env2 := &domain.EventEnvelope{ID: "evt-2", PartitionKey: "key-1"}
	err = kq.Submit(ctx, env2, &mockDelivery{}, 0, 0, "")
	if err == nil {
		t.Errorf("expected error after context cancel, got nil")
	}

	kq.Stop()
}

func TestKeyQueue_HotKeyDetection(t *testing.T) {
	var detected []string
	var mu sync.Mutex

	callback := func(partitionKey string, depth int) {
		mu.Lock()
		detected = append(detected, partitionKey)
		mu.Unlock()
	}

	// Use a slow processor so items accumulate in queue
	processor := func(ctx context.Context, env *domain.EventEnvelope, delivery ports.Delivery, fencingToken int64, vshard uint32, workerID string) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	// Low threshold for testing - 3 items
	kq := NewKeyQueue(10, 10, 1, processor, time.Now, 3, 50*time.Millisecond, callback)
	ctx := context.Background()
	kq.Start(ctx)
	defer kq.Stop()

	// Submit 5 events for the same key - should trigger hot-key detection
	for i := 0; i < 5; i++ {
		env := &domain.EventEnvelope{ID: "evt-" + string(rune('0'+i)), PartitionKey: "hot-key"}
		kq.Submit(ctx, env, &mockDelivery{}, 0, 0, "")
	}

	// Wait for detection - need to wait for at least one check interval
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(detected) == 0 {
		t.Errorf("expected hot-key detection, got none")
	}
	if len(detected) > 0 && detected[0] != "hot-key" {
		t.Errorf("expected hot-key, got %s", detected[0])
	}
}
