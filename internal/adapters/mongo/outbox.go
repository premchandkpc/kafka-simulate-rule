package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/flowrule/flowrule/internal/domain"
)

type OutboxRepository struct {
	querier Querier
}

func NewOutboxRepository(q Querier) *OutboxRepository {
	return &OutboxRepository{querier: q}
}

func (r *OutboxRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

func (r *OutboxRepository) Insert(ctx context.Context, effects []domain.OutboxEffect) error {
	coll := r.collection("outbox_effects")
	if len(effects) == 0 {
		return nil
	}
	
	docs := make([]interface{}, len(effects))
	for i, ef := range effects {
		docs[i] = OutboxDocFromDomain(&ef)
	}
	
	_, err := coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err != nil {
		// Check for duplicate key errors (idempotent insert)
		if mongo.IsDuplicateKeyError(err) {
			return nil
		}
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

func (r *OutboxRepository) ClaimPending(ctx context.Context, batchSize int, owner string, claimTTL time.Duration) ([]domain.OutboxEffect, error) {
	coll := r.collection("outbox_effects")
	now := time.Now().UTC()
	
	filter := bson.M{
		"$or": []bson.M{
			{
				"status":       domain.OutboxStatusPending,
				"available_at": bson.M{"$lte": now},
			},
			{
				"status":            domain.OutboxStatusClaimed,
				"claim_expires_at": bson.M{"$lte": now},
			},
		},
	}
	
	update := bson.M{
		"$set": bson.M{
			"status":           domain.OutboxStatusClaimed,
			"claimed_by":       owner,
			"claimed_at":       now,
			"claim_expires_at": now.Add(claimTTL),
			"updated_at":       now,
		},
	}
	
	opts := options.FindOneAndUpdate().
		SetSort(bson.D{{Key: "available_at", Value: 1}, {Key: "created_at", Value: 1}}).
		SetReturnDocument(options.After)
	
	var effects []domain.OutboxEffect
	for i := 0; i < batchSize; i++ {
		var doc OutboxDoc
		err := coll.FindOneAndUpdate(ctx, filter, bson.M{"$set": update["$set"]}, opts).Decode(&doc)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				break
			}
			return nil, fmt.Errorf("claim pending: %w", err)
		}
		effects = append(effects, *doc.ToDomain())
	}
	
	return effects, nil
}

func (r *OutboxRepository) MarkDelivered(ctx context.Context, effectID string) error {
	coll := r.collection("outbox_effects")
	now := time.Now().UTC()
	
	_, err := coll.UpdateOne(ctx,
		bson.M{
			"effect_id": effectID,
			"status":    domain.OutboxStatusClaimed,
		},
		bson.M{
			"$set": bson.M{
				"status":           domain.OutboxStatusDelivered,
				"claimed_by":       "",
				"claimed_at":       nil,
				"claim_expires_at": nil,
				"updated_at":       now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("mark delivered: %w", err)
	}
	return nil
}

func (r *OutboxRepository) ScheduleRetry(ctx context.Context, effectID string, delay time.Duration, attempts int, errMsg string) error {
	coll := r.collection("outbox_effects")
	now := time.Now().UTC()
	availableAt := now.Add(delay)
	
	_, err := coll.UpdateOne(ctx,
		bson.M{
			"effect_id": effectID,
			"status":    domain.OutboxStatusClaimed,
		},
		bson.M{
			"$set": bson.M{
				"status":           domain.OutboxStatusPending,
				"attempts":         attempts,
				"available_at":     availableAt,
				"last_error":       errMsg,
				"claimed_by":       "",
				"claimed_at":       nil,
				"claim_expires_at": nil,
				"updated_at":       now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("schedule retry: %w", err)
	}
	return nil
}

func (r *OutboxRepository) Quarantine(ctx context.Context, effectID string, errMsg string) error {
	coll := r.collection("outbox_effects")
	now := time.Now().UTC()
	
	_, err := coll.UpdateOne(ctx,
		bson.M{
			"effect_id": effectID,
			"status":    domain.OutboxStatusClaimed,
		},
		bson.M{
			"$set": bson.M{
				"status":     domain.OutboxStatusQuarantined,
				"last_error": errMsg,
				"claimed_by": "",
				"claimed_at": nil,
				"claim_expires_at": nil,
				"updated_at": now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("quarantine effect: %w", err)
	}
	return nil
}

type OutboxDoc struct {
	EffectID       string     `bson:"effect_id"`
	ExecutionID    string     `bson:"execution_id"`
	Destination    string     `bson:"destination"`
	Name           string     `bson:"name"`
	Payload        bson.Raw   `bson:"payload"`
	EffectType     string     `bson:"effect_type"`
	Status         string     `bson:"status"`
	Attempts       int        `bson:"attempts"`
	MaxAttempts    int        `bson:"max_attempts"`
	AvailableAt    time.Time  `bson:"available_at"`
	ClaimedBy      string     `bson:"claimed_by,omitempty"`
	ClaimedAt      *time.Time `bson:"claimed_at,omitempty"`
	ClaimExpiresAt *time.Time `bson:"claim_expires_at,omitempty"`
	LastError      string     `bson:"last_error,omitempty"`
	CreatedAt      time.Time  `bson:"created_at"`
	UpdatedAt      time.Time  `bson:"updated_at"`
}

func OutboxDocFromDomain(ef *domain.OutboxEffect) *OutboxDoc {
	return &OutboxDoc{
		EffectID:       ef.ID,
		ExecutionID:    ef.ExecutionID,
		Destination:    ef.Destination,
		Name:           ef.Name,
		Payload:        bson.Raw(ef.Payload),
		EffectType:     string(ef.EffectType),
		Status:         string(ef.Status),
		Attempts:       ef.Attempts,
		MaxAttempts:    ef.MaxAttempts,
		AvailableAt:    ef.AvailableAt,
		ClaimedBy:      ef.ClaimedBy,
		ClaimedAt:      ef.ClaimedAt,
		ClaimExpiresAt: ef.ClaimExpiresAt,
		LastError:      ef.LastError,
		CreatedAt:      ef.CreatedAt,
		UpdatedAt:      ef.UpdatedAt,
	}
}

func (d *OutboxDoc) ToDomain() *domain.OutboxEffect {
	return &domain.OutboxEffect{
		ID:             d.EffectID,
		ExecutionID:    d.ExecutionID,
		Destination:    d.Destination,
		Name:           d.Name,
		Payload:        json.RawMessage(d.Payload),
		EffectType:     domain.EffectType(d.EffectType),
		Status:         domain.OutboxStatus(d.Status),
		Attempts:       d.Attempts,
		MaxAttempts:    d.MaxAttempts,
		AvailableAt:    d.AvailableAt,
		ClaimedBy:      d.ClaimedBy,
		ClaimedAt:      d.ClaimedAt,
		ClaimExpiresAt: d.ClaimExpiresAt,
		LastError:      d.LastError,
		CreatedAt:      d.CreatedAt,
		UpdatedAt:      d.UpdatedAt,
	}
}

func (r *OutboxRepository) GetByExecution(ctx context.Context, executionID string) ([]domain.OutboxEffect, error) {
	coll := r.collection("outbox_effects")
	
	filter := bson.M{"execution_id": executionID}
	
	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("get outbox by execution: %w", err)
	}
	defer cursor.Close(ctx)
	
	var effects []domain.OutboxEffect
	for cursor.Next(ctx) {
		var doc OutboxDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode outbox effect: %w", err)
		}
		effects = append(effects, *doc.ToDomain())
	}
	
	return effects, nil
}