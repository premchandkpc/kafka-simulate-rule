package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/flowrule/flowrule/internal/domain"
)

type ShardLeaseRepository struct {
	querier Querier
}

func NewShardLeaseRepository(querier Querier) *ShardLeaseRepository {
	return &ShardLeaseRepository{querier: querier}
}

func (r *ShardLeaseRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

func (r *ShardLeaseRepository) Acquire(ctx context.Context, shard uint32, owner string, ttl time.Duration) (*domain.ShardLease, error) {
	coll := r.collection("shard_leases")
	now := time.Now().UTC()
	expiresAt := now.Add(ttl)

	filter := bson.M{
		"virtual_shard": shard,
		"$or": []bson.M{
			{"expires_at": bson.M{"$lt": now}},
			{"owner": owner},
		},
	}

	update := bson.M{
		"$set": bson.M{
			"virtual_shard": shard,
			"owner":         owner,
			"fencing_token": 1,
			"expires_at":    expiresAt,
			"routing_epoch": 0,
		},
		"$inc": bson.M{
			"fencing_token": 1,
		},
		"$setOnInsert": bson.M{
			"virtual_shard": shard,
			"fencing_token": 1,
		},
	}

	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	var doc LeaseDoc
	err := coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
	if err != nil {
		return nil, fmt.Errorf("acquire lease: %w", err)
	}

	if doc.Owner != owner {
		return nil, domain.ErrLeaseOwnedByOther
	}

	return doc.ToDomain(), nil
}

func (r *ShardLeaseRepository) Renew(ctx context.Context, shard uint32, owner string, fencingToken int64, ttl time.Duration) (*domain.ShardLease, error) {
	coll := r.collection("shard_leases")
	now := time.Now().UTC()
	expiresAt := now.Add(ttl)

	filter := bson.M{
		"virtual_shard": shard,
		"owner":         owner,
		"fencing_token": fencingToken,
		"expires_at":    bson.M{"$gt": now},
	}

	update := bson.M{
		"$set": bson.M{
			"expires_at": expiresAt,
		},
	}

	result, err := coll.UpdateOne(ctx, filter, bson.M{"$set": update["$set"]})
	if err != nil {
		return nil, fmt.Errorf("renew lease: %w", err)
	}
	if result.MatchedCount == 0 {
		return nil, domain.ErrLeaseExpired
	}

	return r.GetOwner(ctx, shard)
}

func (r *ShardLeaseRepository) Release(ctx context.Context, shard uint32, owner string) error {
	coll := r.collection("shard_leases")
	_, err := coll.DeleteOne(ctx, bson.M{
		"virtual_shard": shard,
		"owner":         owner,
	})
	if err != nil {
		return fmt.Errorf("release lease: %w", err)
	}
	return nil
}

func (r *ShardLeaseRepository) GetOwner(ctx context.Context, shard uint32) (*domain.ShardLease, error) {
	coll := r.collection("shard_leases")
	var doc LeaseDoc
	err := coll.FindOne(ctx, bson.M{
		"virtual_shard": shard,
	}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("get owner: %w", err)
		}
		return nil, fmt.Errorf("get owner: %w", err)
	}
	return doc.ToDomain(), nil
}

func (r *ShardLeaseRepository) ValidateFencingToken(ctx context.Context, shard uint32, owner string, fencingToken int64) error {
	coll := r.collection("shard_leases")
	var doc LeaseDoc
	err := coll.FindOne(ctx, bson.M{
		"virtual_shard": shard,
		"owner":         owner,
		"fencing_token": fencingToken,
		"expires_at":    bson.M{"$gt": time.Now().UTC()},
	}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return domain.ErrFencingTokenMismatch
		}
		return fmt.Errorf("validate fencing token: %w", err)
	}
	return nil
}

type LeaseDoc struct {
	VirtualShard uint32    `bson:"virtual_shard"`
	Owner        string    `bson:"owner"`
	FencingToken int64     `bson:"fencing_token"`
	ExpiresAt    time.Time `bson:"expires_at"`
	RoutingEpoch int64     `bson:"routing_epoch"`
}

func (d *LeaseDoc) ToDomain() *domain.ShardLease {
	return &domain.ShardLease{
		VirtualShard: d.VirtualShard,
		Owner:        d.Owner,
		FencingToken: d.FencingToken,
		ExpiresAt:    d.ExpiresAt,
		RoutingEpoch: d.RoutingEpoch,
	}
}
