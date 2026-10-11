// Package simulator provides the reusable load-generation use case.
// Transport setup and CLI concerns belong in cmd/simulator.
package simulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
)

const (
	minClients         = 1
	maxClients         = 1000
	minMessages        = 1
	maxMessages        = 100
	minConcurrency     = 1
	maxConcurrency     = 256
	// DefaultBaseSubject is the base subject used by FlowRule event producers.
	DefaultBaseSubject = "events"
)

// Publisher is the smallest transport contract required by the simulator.
type Publisher interface {
	Publish(ctx context.Context, subject string, payload []byte) error
}

// Config bounds one simulation run. NumShards must match the running workers.
type Config struct {
	Clients             int
	MaxMessagesPerClient int
	NumShards           uint32
	Concurrency         int
	BaseSubject         string
}

// Summary distinguishes generated attempts from durable broker acknowledgements.
type Summary struct {
	Clients     int
	Attempted   int
	BrokerAcked int
	Failed      int
	NumShards   uint32
}

type generationResult struct {
	count int
	err   error
}

// Validate rejects unsafe or nonsensical load settings before starting goroutines.
func (c Config) Validate() error {
	switch {
	case c.Clients < minClients || c.Clients > maxClients:
		return fmt.Errorf("clients must be between %d and %d", minClients, maxClients)
	case c.MaxMessagesPerClient < minMessages || c.MaxMessagesPerClient > maxMessages:
		return fmt.Errorf("max messages per client must be between %d and %d", minMessages, maxMessages)
	case c.NumShards == 0:
		return errors.New("shards must be greater than zero")
	case c.Concurrency < minConcurrency || c.Concurrency > maxConcurrency:
		return fmt.Errorf("concurrency must be between %d and %d", minConcurrency, maxConcurrency)
	case c.BaseSubject == "":
		return errors.New("base subject is required")
	}
	return nil
}

// Run generates events and publishes them concurrently. It returns a partial summary
// if ctx is cancelled; a publish error is counted in Summary.Failed, not hidden.
func Run(ctx context.Context, cfg Config, publisher Publisher) (Summary, error) {
	if err := cfg.Validate(); err != nil {
		return Summary{}, err
	}
	if publisher == nil {
		return Summary{}, errors.New("publisher is required")
	}
	if ctx == nil {
		return Summary{}, errors.New("context is required")
	}

	summary := Summary{Clients: cfg.Clients, NumShards: cfg.NumShards}
	jobs := make(chan domain.EventEnvelope, cfg.Concurrency)
	results := make(chan error, cfg.Concurrency)
	generated := make(chan generationResult, 1)

	var workers sync.WaitGroup
	workers.Add(cfg.Concurrency)
	for i := 0; i < cfg.Concurrency; i++ {
		go func() {
			defer workers.Done()
			for event := range jobs {
				if err := publishEvent(ctx, cfg, publisher, event); err != nil {
					results <- err
				} else {
					results <- nil
				}
			}
		}()
	}
	go func() {
		workers.Wait()
		close(results)
	}()

	go func() {
		count, err := generate(ctx, cfg, jobs)
		close(jobs)
		generated <- generationResult{count: count, err: err}
		close(generated)
	}()

	for err := range results {
		if err != nil {
			summary.Failed++
		} else {
			summary.BrokerAcked++
		}
	}
	generation := <-generated
	summary.Attempted = generation.count
	if generation.err != nil {
		return summary, generation.err
	}
	if err := ctx.Err(); err != nil {
		return summary, err
	}
	return summary, nil
}

func generate(ctx context.Context, cfg Config, jobs chan<- domain.EventEnvelope) (int, error) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	attempted := 0

	for client := 1; client <= cfg.Clients; client++ {
		tenantID := fmt.Sprintf("sim-tenant-%03d", client)
		messageCount := rng.Intn(cfg.MaxMessagesPerClient) + 1
		for sequence := 1; sequence <= messageCount; sequence++ {
			partitionKey := fmt.Sprintf("key-%04d", rng.Intn(500))
			payload, err := json.Marshal(struct {
				ClientID string  `json:"client_id"`
				Sequence int     `json:"sequence"`
				Message  string  `json:"message"`
				Amount   float64 `json:"amount"`
			}{
				ClientID: tenantID,
				Sequence: sequence,
				Message:  "random-payload-" + domain.NewID(),
				Amount:   float64(rng.Intn(100000)) / 100,
			})
			if err != nil {
				return attempted, fmt.Errorf("marshal generated payload for tenant %s: %w", tenantID, err)
			}

			event := domain.EventEnvelope{
				ID:           domain.NewID(),
				Type:         "simulation.message",
				TenantID:     tenantID,
				PartitionKey: partitionKey,
				OccurredAt:   time.Now().UTC(),
				Data:         payload,
				Headers:      map[string]string{"source": "random-producer-simulator"},
			}
			select {
			case jobs <- event:
				attempted++
			case <-ctx.Done():
				return attempted, nil
			}
		}
	}
	return attempted, nil
}

func publishEvent(ctx context.Context, cfg Config, publisher Publisher, event domain.EventEnvelope) error {
	if err := event.Validate(); err != nil {
		return fmt.Errorf("validate generated event: %w", err)
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal generated event %q: %w", event.ID, err)
	}
	shard := event.VirtualShard(cfg.NumShards)
	subject := fmt.Sprintf("%s.shard.%d", cfg.BaseSubject, shard)
	if err := publisher.Publish(ctx, subject, payload); err != nil {
		return fmt.Errorf("publish event %q to %s: %w", event.ID, subject, err)
	}
	return nil
}
