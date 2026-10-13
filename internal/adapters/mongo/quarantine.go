package mongo

import (
	"context"
	"errors"
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
	return &QuarantineRepository{querier: querier}
}

func (r *QuarantineRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

// BackfillEffectTenantIDs repairs legacy effect quarantine records using their
// outbox effect and execution as the authoritative tenant source. Records whose
// source chain no longer exists are left unscoped and remain invisible to tenants.
func (r *QuarantineRepository) BackfillEffectTenantIDs(ctx context.Context) error {
	coll := r.collection(collectionQuarantine)
	cursor, err := coll.Find(ctx, bson.D{
		{Key: "source_type", Value: "effect"},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "tenant_id", Value: bson.D{{Key: "$exists", Value: false}}}},
			bson.D{{Key: "tenant_id", Value: ""}},
		}},
	})
	if err != nil {
		return fmt.Errorf("find legacy effect quarantine entries: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	for cursor.Next(ctx) {
		var entry QuarantineDoc
		if err := cursor.Decode(&entry); err != nil {
			return fmt.Errorf("decode legacy effect quarantine entry: %w", err)
		}
		var effect struct {
			ExecutionID string `bson:"execution_id"`
		}
		if err := r.collection(collectionOutboxEffects).FindOne(ctx, bson.D{
			{Key: "effect_id", Value: entry.SourceID},
		}).Decode(&effect); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				continue
			}
			return fmt.Errorf("find outbox effect %s: %w", entry.SourceID, err)
		}
		var execution struct {
			TenantID string `bson:"tenant_id"`
		}
		if err := r.collection(collectionExecutions).FindOne(ctx, bson.D{
			{Key: "execution_id", Value: effect.ExecutionID},
		}).Decode(&execution); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				continue
			}
			return fmt.Errorf("find execution %s: %w", effect.ExecutionID, err)
		}
		if execution.TenantID == "" {
			continue
		}
		_, err := coll.UpdateOne(ctx,
			bson.D{
				{Key: "quarantine_id", Value: entry.QuarantineID},
				{Key: "$or", Value: bson.A{
					bson.D{{Key: "tenant_id", Value: bson.D{{Key: "$exists", Value: false}}}},
					bson.D{{Key: "tenant_id", Value: ""}},
				}},
			},
			bson.D{{Key: "$set", Value: bson.D{{Key: "tenant_id", Value: execution.TenantID}}}},
		)
		if err != nil {
			return fmt.Errorf("backfill tenant for quarantine entry %s: %w", entry.QuarantineID, err)
		}
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("iterate legacy effect quarantine entries: %w", err)
	}
	return nil
}

func (r *QuarantineRepository) Save(ctx context.Context, entry *domain.QuarantineEntry) error {
	coll := r.collection(collectionQuarantine)
	doc := QuarantineDocFromDomain(entry)
	_, err := coll.UpdateOne(ctx,
		bson.D{{Key: "quarantine_id", Value: entry.ID}},
		bson.D{{Key: "$setOnInsert", Value: doc}},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("save quarantine entry: %w", err)
	}
	return nil
}

func (r *QuarantineRepository) Get(ctx context.Context, tenantID, id string) (*domain.QuarantineEntry, error) {
	if tenantID == "" {
		return nil, nil
	}
	coll := r.collection(collectionQuarantine)
	var doc QuarantineDoc
	err := coll.FindOne(ctx, bson.D{
		{Key: "quarantine_id", Value: id},
		{Key: "tenant_id", Value: tenantID},
	}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get quarantine entry: %w", err)
	}
	return doc.ToDomain(), nil
}

func (r *QuarantineRepository) Delete(ctx context.Context, tenantID, id string) error {
	if tenantID == "" {
		return domain.ErrQuarantineNotFound
	}
	coll := r.collection(collectionQuarantine)
	result, err := coll.DeleteOne(ctx, bson.D{{Key: "quarantine_id", Value: id}, {Key: "tenant_id", Value: tenantID}})
	if err != nil {
		return fmt.Errorf("delete quarantine entry: %w", err)
	}
	if result.DeletedCount == 0 {
		return domain.ErrQuarantineNotFound
	}
	return nil
}

func (r *QuarantineRepository) List(ctx context.Context, filter ports.QuarantineFilter) ([]*domain.QuarantineEntry, error) {
	if filter.TenantID == "" {
		return nil, fmt.Errorf("tenant_id is required")
	}
	filter.Limit, filter.Offset = normalizeQuarantinePagination(filter.Limit, filter.Offset)
	coll := r.collection(collectionQuarantine)

	// Keep the query shape fixed and treat every caller-supplied filter as a
	// literal equality operand. In particular, values must never become MongoDB
	// query operators or nested query documents.
	f := bson.D{{Key: "tenant_id", Value: mongoEquality(filter.TenantID)}}
	if filter.SourceType != "" {
		f = append(f, bson.E{Key: "source_type", Value: mongoEquality(filter.SourceType)})
	}
	if filter.ErrorClass != "" {
		f = append(f, bson.E{Key: "error_class", Value: mongoEquality(filter.ErrorClass)})
	}
	if !filter.From.IsZero() || !filter.To.IsZero() {
		dateRange := bson.D{}
		if !filter.From.IsZero() {
			dateRange = append(dateRange, bson.E{Key: "$gte", Value: filter.From})
		}
		if !filter.To.IsZero() {
			dateRange = append(dateRange, bson.E{Key: "$lte", Value: filter.To})
		}
		f = append(f, bson.E{Key: "created_at", Value: dateRange})
	}

	cursor, err := coll.Find(ctx, f, options.Find().
		SetLimit(int64(filter.Limit)).
		SetSkip(int64(filter.Offset)).
		SetSort(bson.D{{Key: "created_at", Value: -1}}))
	if err != nil {
		return nil, fmt.Errorf("list quarantine: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var entries []*domain.QuarantineEntry
	for cursor.Next(ctx) {
		var doc QuarantineDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode quarantine: %w", err)
		}
		entries = append(entries, doc.ToDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate quarantine: %w", err)
	}

	return entries, nil
}

func (r *QuarantineRepository) Replay(ctx context.Context, tenantID, id string) error {
	if tenantID == "" {
		return domain.ErrQuarantineNotFound
	}
	coll := r.collection(collectionQuarantine)
	now := time.Now().UTC()

	result, err := coll.UpdateOne(ctx,
		bson.D{
			{Key: "quarantine_id", Value: id},
			{Key: "tenant_id", Value: tenantID},
		},
		bson.D{{Key: "$set", Value: bson.D{{Key: "updated_at", Value: now}}}},
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
	QuarantineID string    `bson:"quarantine_id"`
	SourceType   string    `bson:"source_type"`
	SourceID     string    `bson:"source_id"`
	ErrorClass   string    `bson:"error_class"`
	PayloadRef   string    `bson:"payload_ref,omitempty"`
	EventID      string    `bson:"event_id,omitempty"`
	TenantID     string    `bson:"tenant_id,omitempty"`
	Revision     *int64    `bson:"revision,omitempty"`
	DecisionHash string    `bson:"decision_hash,omitempty"`
	ErrorMessage string    `bson:"error_message,omitempty"`
	CreatedAt    time.Time `bson:"created_at"`
	UpdatedAt    time.Time `bson:"updated_at"`
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

// mongoEquality wraps an untrusted value in a fixed equality expression so it
// is always interpreted as data, never as caller-controlled query syntax.
func mongoEquality(value any) bson.D {
	return bson.D{{Key: "$eq", Value: value}}
}

func normalizeQuarantinePagination(limit, offset int) (int, int) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}
