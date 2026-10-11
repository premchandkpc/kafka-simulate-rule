package config

import (
	"os"
	"testing"
	"time"
)

func validConfigForTest() *Config {
	return &Config{
		App:      AppConfig{HTTPAddr: "127.0.0.1:8080", ShutdownTimeout: time.Second},
		Database: DatabaseConfig{StorageBackend: "postgres", DSN: "postgres://localhost/flowrule"},
		NATS:     NATSConfig{URL: "nats://localhost:4222"},
		Worker: WorkerConfig{
			NumShards:               1,
			MaxGlobalInFlight:       1,
			FetchBatchSize:          1,
			EffectPublishIntervalMS: 1,
			EffectPublishBatchSize:  1,
			RetryDelayMS:            1,
			MaxPerKeyQueue:          1,
			KeyQueueWorkers:         1,
			BatchMax:                1,
			BatchWindowMS:           1,
			SchedulerPollMS:         1,
			SchedulerBatchSize:      1,
			LeaseTTLMS:              3,
			LeaseRenewalIntervalMS:  1,
			NATSAckWaitMS:           1500,
			NATSMaxDeliver:          1,
			HotKeyThreshold:         1,
			HotKeyCheckIntervalMS:   2500,
		},
	}
}

func TestValidateRejectsInvalidWorkerRuntimeSettings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "fetch batch exceeds in-flight limit", mutate: func(c *Config) { c.Worker.FetchBatchSize = 2 }},
		{name: "lease renewal exceeds safe interval", mutate: func(c *Config) { c.Worker.LeaseRenewalIntervalMS = 2 }},
		{name: "NATS ack wait", mutate: func(c *Config) { c.Worker.NATSAckWaitMS = 0 }},
		{name: "NATS max deliver", mutate: func(c *Config) { c.Worker.NATSMaxDeliver = 0 }},
		{name: "hot key threshold", mutate: func(c *Config) { c.Worker.HotKeyThreshold = 0 }},
		{name: "hot key check interval", mutate: func(c *Config) { c.Worker.HotKeyCheckIntervalMS = 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfigForTest()
			tt.mutate(cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want invalid setting error")
			}
		})
	}
}

func TestWorkerRuntimeDurationHelpers(t *testing.T) {
	cfg := validConfigForTest()

	if got, want := cfg.NATSAckWait(), 1500*time.Millisecond; got != want {
		t.Fatalf("NATSAckWait() = %s, want %s", got, want)
	}
	if got, want := cfg.HotKeyCheckInterval(), 2500*time.Millisecond; got != want {
		t.Fatalf("HotKeyCheckInterval() = %s, want %s", got, want)
	}
}

func TestLoadRejectsMalformedWorkerRuntimeSetting(t *testing.T) {
	t.Setenv("NATS_ACK_WAIT_MS", "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want malformed integer error")
	}
}

func TestLoadRejectsMalformedShutdownTimeout(t *testing.T) {
	t.Setenv("SHUTDOWN_TIMEOUT", "later-ish")
	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want malformed duration error")
	}
}

func TestLoadDefaultsHTTPAddrToLoopback(t *testing.T) {
	previous, wasSet := os.LookupEnv("HTTP_ADDR")
	if err := os.Unsetenv("HTTP_ADDR"); err != nil {
		t.Fatalf("Unsetenv(HTTP_ADDR): %v", err)
	}
	t.Cleanup(func() {
		if wasSet {
			_ = os.Setenv("HTTP_ADDR", previous)
			return
		}
		_ = os.Unsetenv("HTTP_ADDR")
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got, want := cfg.App.HTTPAddr, "127.0.0.1:8080"; got != want {
		t.Fatalf("HTTPAddr = %q, want %q", got, want)
	}
}
