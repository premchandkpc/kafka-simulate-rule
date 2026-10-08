package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type QuarantineRepository struct {
	querier Querier
}

func NewQuarantineRepository(querier Querier) *QuarantineRepository {
	return &QuarantineRepository{db: db}
}

func (r *QuarantineRepository) Save(ctx context.Context, entry *domain.QuarantineEntry) error {
	coll := r.collection("quarantine")
	doc := QuarantineDocFromDomain(entry)
	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("save quarantine entry: %w", err)
	}
	return nil
}

func (r *QuarantineRepository) Get(ctx context.Context, id string) (*domain.QuarantineEntry, error) {
	coll := r.collection("quarantine")
	var doc QuarantineDoc
	err := coll.FindOne(ctx, bson.M{
		"quarantine_id": id,
	}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("get quarantine entry: %w", err)
	}
	return doc.ToDomain(), nil
}

func (r *QuarantineRepository) Replay(ctx context.Context, id string) error {
	coll := r.collection("quarantine")
	now := time.Now().UTC()
	
	result, err := coll.UpdateOne(ctx,
		bson.M{
			"quarantine_id": id,
		},
		bson.M{
			"$set": bson.M{
				"updated_at": now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("record quarantine replay: %w", err)
	}
	if result.MatchedCount == 0 {
		return domain.ErrQuarantineNotFound
	}
	return nil
}

type QuarantineDoc struct {
	QuarantineID  string     `bson:"quarantine_id"`
	SourceType    string     `bson:"source_type"`
	SourceID      string     `bson:"source_id"`
	ErrorClass    string     `bson:"error_class"`
	PayloadRef    string     `bson:"payload_ref,omitempty"`
	EventID       string     `bson:"event_id,omitempty"`
	TenantID      string     `bson:"tenant_id,omitempty"`
	Revision      *int64     `bson:"revision,omitempty"`
	DecisionHash  string     `bson:"decision_hash,omitempty"`
	ErrorMessage  string     `bson:"error_message,omitempty"`
	CreatedAt     time.Time  `bson:"created_at"`
	UpdatedAt     time.Time  `bson:"updated_at"`
}

func QuarantineDocFromDomain(entry *domain.QuarantineEntry) *QuarantineDoc {
	return &QuarantineDoc{
		QuarantineID: entry.ID,
		SourceType:   entry.SourceType,
		SourceID:     entry.SourceID,
		ErrorClass:   entry.ErrorClass,
		PayloadRef:   entry.PayloadRef,
		EventID:      entry.EventID,
		TenantID:     entry.TenantID,
		ErrorMessage: entry.Error,
		CreatedAt:    entry.CreatedAt,
		UpdatedAt:    entry.CreatedAt,
	}
}

func (d *QuarantineDoc) ToDomain() *domain.QuarantineEntry {
	return &domain.QuarantineEntry{
		ID:         d.QuarantineID,
		SourceType: d.SourceType,
		SourceID:   d.SourceID,
		ErrorClass: d.ErrorClass,
		PayloadRef: d.PayloadRef,
		EventID:    d.EventID,
		TenantID:   d.TenantID,
		Error:      d.ErrorMessage,
		CreatedAt:  d.CreatedAt,
	}
}