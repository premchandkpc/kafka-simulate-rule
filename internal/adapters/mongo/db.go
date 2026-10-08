package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DB struct {
	client   *mongo.Client
	database *mongo.Database
}

type Config struct {
	URI        string
	Database   string
	MaxPool    uint64
	MinPool    uint64
	MaxConnIdle time.Duration
}

func New(ctx context.Context, cfg Config) (*DB, error) {
	if cfg.URI == "" {
		cfg.URI = "mongodb://localhost:27017"
	}
	if cfg.Database == "" {
		cfg.Database = "flowrule"
	}
	if cfg.MaxPool == 0 {
		cfg.MaxPool = 20
	}
	if cfg.MinPool == 0 {
		cfg.MinPool = 2
	}
	if cfg.MaxConnIdle == 0 {
		cfg.MaxConnIdle = 5 * time.Minute
	}

	clientOpts := options.Client().
		ApplyURI(cfg.URI).
		SetMaxPoolSize(cfg.MaxPool).
		SetMinPoolSize(cfg.MinPool).
		SetMaxConnIdleTime(cfg.MaxConnIdle)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("connect mongo: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("ping mongo: %w", err)
	}

	db := client.Database(cfg.Database)

	// Create indexes
	if err := createIndexes(ctx, db); err != nil {
		return nil, fmt.Errorf("create indexes: %w", err)
	}

	return &DB{client: client, database: db}, nil
}

func (d *DB) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.client.Disconnect(ctx)
}

func (d *DB) Database() *mongo.Database {
	return d.database
}

func (d *DB) Client() *mongo.Client {
	return d.client
}

func createIndexes(ctx context.Context, db *mongo.Database) error {
	indexes := map[string][]mongo.IndexModel{
		"rule_revisions": {
			{Keys: bson.D{{Key: "tenant_scope", Value: 1}, {Key: "rule_id", Value: 1}, {Key: "revision", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "content_hash", Value: 1}}},
		},
		"rule_activations": {
			{Keys: bson.D{{Key: "tenant_scope", Value: 1}, {Key: "rule_set", Value: 1}}, Options: options.Index().SetUnique(true)},
		},
		"inbox": {
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "event_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "partition_key", Value: 1}, {Key: "first_seen_at", Value: 1}}, Options: options.Index().SetPartialFilterExpression(bson.M{"status": "committed", "batch_id": bson.M{"$exists": false}})},
		},
		"executions": {
			{Keys: bson.D{{Key: "execution_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "event_id", Value: 1}}},
			{Keys: bson.D{{Key: "status", Value: 1}}},
		},
		"outbox_effects": {
			{Keys: bson.D{{Key: "effect_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "execution_id", Value: 1}}},
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "available_at", Value: 1}}},
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "claim_expires_at", Value: 1}}, Options: options.Index().SetPartialFilterExpression(bson.M{"status": "claimed"})},
		},
		"shard_leases": {
			{Keys: bson.D{{Key: "virtual_shard", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "owner", Value: 1}}},
			{Keys: bson.D{{Key: "expires_at", Value: 1}}},
		},
		"quarantine": {
			{Keys: bson.D{{Key: "quarantine_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "tenant_id", Value: 1}}},
			{Keys: bson.D{{Key: "created_at", Value: 1}}},
		},
		"batch_runs": {
			{Keys: bson.D{{Key: "batch_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "created_at", Value: 1}}},
		},
		"scheduled_events": {
			{Keys: bson.D{{Key: "event_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "scheduled_at", Value: 1}}, Options: options.Index().SetPartialFilterExpression(bson.M{"status": "pending"})},
		},
		"workflow_instances": {
			{Keys: bson.D{{Key: "workflow_id", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "state", Value: 1}}},
			{Keys: bson.D{{Key: "updated_at", Value: 1}}},
		},
	}

	for collName, models := range indexes {
		coll := db.Collection(collName)
		_, err := coll.Indexes().CreateMany(ctx, models)
		if err != nil {
			return fmt.Errorf("create indexes for %s: %w", collName, err)
		}
	}
	return nil
}