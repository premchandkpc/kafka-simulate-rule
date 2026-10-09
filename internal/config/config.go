package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type AppConfig struct {
	Env            string
	LogLevel       string
	HTTPAddr       string
	GRPCAddr       string
	ShutdownTimeout time.Duration
}

type DatabaseConfig struct {
	DSN           string
	MigrationsDir string
	MongoDBURI    string
	MongoDBDatabase string
}

type NATSConfig struct {
	URL      string
	Stream   string
	Consumer string
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type WorkerConfig struct {
	NumShards              uint32
	WorkerID               string
	MaxGlobalInFlight      int
	MaxPerKeyQueue         int
	KeyQueueWorkers        int
	BatchMode              string
	BatchMax               int
	BatchWindowMS          int
	SchedulerPollMS        int
	SchedulerBatchSize     int
	LeaseTTLMS             int
	LeaseRenewalIntervalMS int
}

type GenerationConfig struct {
	ProtocPath                 string
	ProtocGenGoPath            string
	ProtocGenGoGRPCPath        string
	GenerationOutputDir        string
}

type Config struct {
	App         AppConfig
	Database    DatabaseConfig
	NATS        NATSConfig
	Redis       RedisConfig
	Worker      WorkerConfig
	Generation  GenerationConfig
}

func Load() (*Config, error) {
	cfg := &Config{
		App: AppConfig{
			Env:             getEnv("APP_ENV", "development"),
			LogLevel:        getEnv("LOG_LEVEL", "info"),
			HTTPAddr:        getEnv("HTTP_ADDR", ":8080"),
			GRPCAddr:        getEnv("GRPC_ADDR", ":9090"),
			ShutdownTimeout: getDurationEnv("SHUTDOWN_TIMEOUT", 30*time.Second),
		},
		Database: DatabaseConfig{
			DSN:            getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/flowrule?sslmode=disable"),
			MigrationsDir:  getEnv("MIGRATIONS_DIR", "migrations"),
			MongoDBURI:     getEnv("MONGODB_URI", ""),
			MongoDBDatabase: getEnv("MONGODB_DATABASE", ""),
		},
		NATS: NATSConfig{
			URL:      getEnv("NATS_URL", "nats://localhost:4222"),
			Stream:   getEnv("NATS_STREAM", "flowrule"),
			Consumer: getEnv("NATS_CONSUMER", ""),
		},
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       getIntEnv("REDIS_DB", 0),
		},
		Worker: WorkerConfig{
			NumShards:              uint32(getIntEnv("NUM_SHARDS", 4096)),
			WorkerID:               getEnv("WORKER_ID", ""),
			MaxGlobalInFlight:      getIntEnv("MAX_GLOBAL_IN_FLIGHT", 100),
			MaxPerKeyQueue:         getIntEnv("MAX_PER_KEY_QUEUE", 100),
			KeyQueueWorkers:        getIntEnv("KEY_QUEUE_WORKERS", 1),
			BatchMode:              getEnv("BATCH_MODE", "none"),
			BatchMax:               getIntEnv("BATCH_MAX", 100),
			BatchWindowMS:          getIntEnv("BATCH_WINDOW_MS", 30000),
			SchedulerPollMS:        getIntEnv("SCHEDULER_POLL_MS", 5000),
			SchedulerBatchSize:     getIntEnv("SCHEDULER_BATCH_SIZE", 100),
			LeaseTTLMS:             getIntEnv("LEASE_TTL_MS", 30000),
			LeaseRenewalIntervalMS: getIntEnv("LEASE_RENEWAL_INTERVAL_MS", 10000),
		},
		Generation: GenerationConfig{
			ProtocPath:              getEnv("PROTOC_PATH", "protoc"),
			ProtocGenGoPath:         getEnv("PROTOC_GEN_GO_PATH", "protoc-gen-go"),
			ProtocGenGoGRPCPath:     getEnv("PROTOC_GEN_GO_GRPC_PATH", "protoc-gen-go-grpc"),
			GenerationOutputDir:     getEnv("GENERATION_OUTPUT_DIR", ""),
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.App.HTTPAddr == "" {
		return fmt.Errorf("HTTP_ADDR is required")
	}
	if c.App.GRPCAddr == "" {
		return fmt.Errorf("GRPC_ADDR is required")
	}
	if c.Database.DSN == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if c.NATS.URL == "" {
		return fmt.Errorf("NATS_URL is required")
	}
	if c.Worker.NumShards == 0 {
		return fmt.Errorf("NUM_SHARDS must be > 0")
	}
	if c.Worker.MaxGlobalInFlight <= 0 {
		return fmt.Errorf("MAX_GLOBAL_IN_FLIGHT must be > 0")
	}
	if c.Worker.MaxPerKeyQueue <= 0 {
		return fmt.Errorf("MAX_PER_KEY_QUEUE must be > 0")
	}
	if c.Worker.KeyQueueWorkers <= 0 {
		return fmt.Errorf("KEY_QUEUE_WORKERS must be > 0")
	}
	if c.Worker.BatchMax <= 0 {
		return fmt.Errorf("BATCH_MAX must be > 0")
	}
	if c.Worker.BatchWindowMS <= 0 {
		return fmt.Errorf("BATCH_WINDOW_MS must be > 0")
	}
	if c.Worker.SchedulerPollMS <= 0 {
		return fmt.Errorf("SCHEDULER_POLL_MS must be > 0")
	}
	if c.Worker.SchedulerBatchSize <= 0 {
		return fmt.Errorf("SCHEDULER_BATCH_SIZE must be > 0")
	}
	if c.Worker.LeaseTTLMS <= 0 {
		return fmt.Errorf("LEASE_TTL_MS must be > 0")
	}
	if c.Worker.LeaseRenewalIntervalMS <= 0 {
		return fmt.Errorf("LEASE_RENEWAL_INTERVAL_MS must be > 0")
	}
	return nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getIntEnv(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return fallback
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	if value, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return fallback
}

func (c *Config) WorkerID() string {
	if c.Worker.WorkerID != "" {
		return c.Worker.WorkerID
	}
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

func (c *Config) BatchWindow() time.Duration {
	return time.Duration(c.Worker.BatchWindowMS) * time.Millisecond
}

func (c *Config) SchedulerPollInterval() time.Duration {
	return time.Duration(c.Worker.SchedulerPollMS) * time.Millisecond
}

func (c *Config) LeaseTTL() time.Duration {
	return time.Duration(c.Worker.LeaseTTLMS) * time.Millisecond
}

func (c *Config) LeaseRenewalInterval() time.Duration {
	return time.Duration(c.Worker.LeaseRenewalIntervalMS) * time.Millisecond
}

func (c *Config) ShutdownTimeoutDuration() time.Duration {
	return c.App.ShutdownTimeout
}