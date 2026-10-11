package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	natslib "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/flowrule/flowrule/internal/simulator"
)

const (
	defaultNATSURL = "nats://nats:4222"
	connectTimeout = 5 * time.Second
)

type publishFunc func(context.Context, string, []byte) error

func (p publishFunc) Publish(ctx context.Context, subject string, payload []byte) error {
	return p(ctx, subject, payload)
}

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	clients := flag.Int("clients", envInt("SIM_CLIENTS", 10), "number of simulated tenant/client identities")
	maxMessages := flag.Int("max-messages", envInt("SIM_MESSAGES_MAX", 100), "maximum random messages per client (1-100)")
	shards := flag.Uint64("shards", uint64(envInt("NUM_SHARDS", 4096)), "virtual shard count; must match the workers")
	natsURL := flag.String("nats", env("NATS_URL", defaultNATSURL), "NATS URL; use the Compose service name inside the network")
	timeout := flag.Duration("timeout", 30*time.Second, "overall simulation timeout")
	concurrency := flag.Int("concurrency", envInt("SIM_CONCURRENCY", 16), "maximum concurrent publishers")
	flag.Parse()

	if *shards == 0 || *shards > uint64(^uint32(0)) {
		return fmt.Errorf("shards must be between 1 and %d", uint64(^uint32(0)))
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be greater than zero")
	}
	cfg := simulator.Config{
		Clients:              *clients,
		MaxMessagesPerClient: *maxMessages,
		NumShards:            uint32(*shards),
		Concurrency:          *concurrency,
		BaseSubject:          simulator.DefaultBaseSubject,
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	conn, err := natslib.Connect(*natsURL,
		natslib.Name("flowrule-random-producer"),
		natslib.Timeout(connectTimeout),
	)
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	defer conn.Close()

	js, err := jetstream.New(conn)
	if err != nil {
		return fmt.Errorf("create JetStream client: %w", err)
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, *timeout)
	defer cancel()

	publisher := publishFunc(func(ctx context.Context, subject string, payload []byte) error {
		_, err := js.Publish(ctx, subject, payload)
		return err
	})
	summary, runErr := simulator.Run(ctx, cfg, publisher)
	log.Printf("simulation complete: clients=%d attempted=%d broker_acked=%d failed=%d shards=%d",
		summary.Clients, summary.Attempted, summary.BrokerAcked, summary.Failed, summary.NumShards)
	if runErr != nil {
		return fmt.Errorf("simulation stopped: %w", runErr)
	}
	if summary.Failed > 0 {
		return fmt.Errorf("%d of %d publish attempts failed", summary.Failed, summary.Attempted)
	}
	return nil
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
