package effects

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/google/uuid"
)

type FakeDestination struct {
	mu       sync.Mutex
	sent     map[string]bool
	failOn   map[string]bool
	failCount int
}

func NewFakeDestination() *FakeDestination {
	return &FakeDestination{
		sent:   make(map[string]bool),
		failOn: make(map[string]bool),
	}
}

func (f *FakeDestination) Send(ctx context.Context, effect *domain.Effect) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failOn[effect.ID] {
		f.failCount++
		if f.failCount > 3 {
			delete(f.failOn, effect.ID)
			f.failCount = 0
			return nil
		}
		return fmt.Errorf("fake destination error for effect %s", effect.ID)
	}

	f.sent[effect.ID] = true
	log.Printf("FakeDestination: sent effect %s to %s", effect.ID, effect.Destination)
	return nil
}

func (f *FakeDestination) WasSent(effectID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sent[effectID]
}

func (f *FakeDestination) SentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

func (f *FakeDestination) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = make(map[string]bool)
	f.failOn = make(map[string]bool)
	f.failCount = 0
}

func (f *FakeDestination) FailOn(effectID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failOn[effectID] = true
}

type HTTPDestination struct {
	mu       sync.Mutex
	sent     map[string]bool
	endpoint string
	client   *http.Client
}

func NewHTTPDestination(endpoint string) *HTTPDestination {
	return &HTTPDestination{
		sent:     make(map[string]bool),
		endpoint: endpoint,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (h *HTTPDestination) Send(ctx context.Context, effect *domain.Effect) error {
	data, err := json.Marshal(effect)
	if err != nil {
		return fmt.Errorf("marshal effect: %w", err)
	}

	url := h.endpoint + "/" + effect.Destination + "/" + effect.Name
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Effect-ID", effect.ID)
	req.Header.Set("X-Execution-ID", effect.ExecutionID)

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("send effect: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("destination returned %d: %s", resp.StatusCode, string(body))
	}

	h.mu.Lock()
	h.sent[effect.ID] = true
	h.mu.Unlock()

	log.Printf("HTTPDestination: sent effect %s to %s", effect.ID, url)
	return nil
}

func (h *HTTPDestination) WasSent(effectID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sent[effectID]
}

type FakeIDProvider struct{}

func (FakeIDProvider) New() string {
	return uuid.New().String()
}

type FakeClock struct {
	now time.Time
}

func NewFakeClock(now time.Time) *FakeClock {
	return &FakeClock{now: now}
}

func (f *FakeClock) Now() time.Time {
	return f.now
}

func (f *FakeClock) Advance(d time.Duration) {
	f.now = f.now.Add(d)
}