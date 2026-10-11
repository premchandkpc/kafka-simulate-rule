package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	natslib "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type envelope struct {
	ID           string            `json:"id"`
	Type         string            `json:"type"`
	TenantID     string            `json:"tenant_id"`
	PartitionKey string            `json:"partition_key"`
	OccurredAt   time.Time         `json:"occurred_at"`
	Data         map[string]any    `json:"data"`
	Headers      map[string]string `json:"headers,omitempty"`
}

type result struct {
	published int
	failed    int
}

func main() {
	clients := flag.Int("clients", envInt("SIM_CLIENTS", 10), "number of simulated client identities")
	maxMessages := flag.Int("max-messages", envInt("SIM_MESSAGES_MAX", 100), "maximum random messages per client (1-100)")
	shards := flag.Uint("shards", uint(envInt("NUM_SHARDS", 4096)), "virtual shard count; must match the workers")
	natsURL := flag.String("nats", env("NATS_URL", "nats://nats:4222"), "NATS URL; use the Compose service name inside the network")
	timeout := flag.Duration("timeout", 30*time.Second, "overall simulation timeout")
	concurrency := flag.Int("concurrency", 16, "maximum concurrent publishers")
	flag.Parse()

	if *clients < 1 || *clients > 1000 {
		log.Fatal("clients must be between 1 and 1000")
	}
	if *maxMessages < 1 || *maxMessages > 100 {
		log.Fatal("max-messages must be between 1 and 100")
	}
	if *shards < 1 {
		log.Fatal("shards must be greater than zero")
	}
	if *concurrency < 1 || *concurrency > 256 {
		log.Fatal("concurrency must be between 1 and 256")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	conn, err := natslib.Connect(*natsURL, natslib.Name("flowrule-random-producer"))
	if err != nil {
		log.Fatalf("connect to NATS: %v", err)
	}
	defer conn.Close()

	js, err := jetstream.New(conn)
	if err != nil {
		log.Fatalf("create JetStream client: %v", err)
	}

	jobs := make(chan envelope)
	results := make(chan error)
	var workers sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for event := range jobs {
				data, marshalErr := json.Marshal(event)
				if marshalErr != nil {
					results <- marshalErr
					continue
				}
				shard := virtualShard(event.TenantID, event.PartitionKey, uint32(*shards))
				subject := fmt.Sprintf("events.shard.%d", shard)
				// Publish returns only after JetStream acknowledges durable acceptance.
				_, publishErr := js.Publish(ctx, subject, data)
				results <- publishErr
			}
		}()
	}

	total := 0
	go func() {
		defer close(jobs)
		for client := 0; client < *clients; client++ {
			tenantID := fmt.Sprintf("sim-tenant-%03d", client+1)
			count := rand.Intn(*maxMessages) + 1
			for sequence := 1; sequence <= count; sequence++ {
				key := fmt.Sprintf("key-%04d", rand.Intn(500))
				jobs <- envelope{
					ID:           uuid.NewString(),
					Type:         "simulation.message",
					TenantID:     tenantID,
					PartitionKey: key,
					OccurredAt:   time.Now().UTC(),
					Data: map[string]any{
						"client_id": tenantID,
						"sequence":  sequence,
						"message":   fmt.Sprintf("random-payload-%s", uuid.NewString()),
						"amount":    rand.Intn(100000) / 100.0,
					},
					Headers: map[string]string{"source": "random-producer-simulator"},
				}
				total++
			}
		}
	}()

	go func() {
		workers.Wait()
		close(results)
	}()

	published, failed := 0, 0
	for publishErr := range results {
		if publishErr != nil {
			failed++
			log.Printf("publish failed: %v", publishErr)
		} else {
			published++
		}
	}
	log.Printf("simulation complete: clients=%d attempted=%d broker_acked=%d failed=%d shards=%d", *clients, total, published, failed, *shards)
	if failed > 0 {
		os.Exit(1)
	}
}

func virtualShard(tenantID, partitionKey string, shards uint32) uint32 {
	// Must remain identical to domain.EventEnvelope.VirtualShard.
	hash := uint32(2166136261)
	for _, b := range []byte(tenantID + ":" + partitionKey) {
		hash ^= uint32(b)
		hash *= 16777619
	}
	return hash % shards
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("%s must be an integer: %v", key, err)
	}
	return parsed
}
