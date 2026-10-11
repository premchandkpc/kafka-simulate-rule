package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/flowrule/flowrule/internal/domain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ScheduledEventRepository struct {
	querier Querier
}

func NewScheduledEventRepository(q Querier) *ScheduledEventRepository {
	return &ScheduledEventRepository{querier: q}
}

func (r *ScheduledEventRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

func (r *ScheduledEventRepository) Save(ctx context.Context, event *domain.ScheduledEvent) error {
	coll := r.collection("scheduled_events")
	doc := ScheduledDocFromDomain(event)
	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil
		}
		return fmt.Errorf("save scheduled event: %w", err)
	}
	return nil
}

func (r *ScheduledEventRepository) GetDue(ctx context.Context, before time.Time, limit int) ([]*domain.ScheduledEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	coll := r.collection("scheduled_events")
	filter := bson.M{
		"$or": []bson.M{
			{"status": "pending", "scheduled_at": bson.M{"$lte": before}},
			{"status": "claimed", "claim_expires_at": bson.M{"$lte": before}},
		},
	}
	cursor, err := coll.Find(ctx, filter, options.Find().
		SetSort(bson.D{{Key: "scheduled_at", Value: 1}}).
		SetLimit(int64(limit)))
	if err != nil {
		return nil, fmt.Errorf("get due scheduled events: %w", err)
	}
	defer cursor.Close(ctx)

	var events []*domain.ScheduledEvent
	for cursor.Next(ctx) {
		var doc ScheduledDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode scheduled event: %w", err)
		}
		events = append(events, doc.ToDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate scheduled events: %w", err)
	}
	return events, nil
}

func (r *ScheduledEventRepository) Claim(ctx context.Context, eventIDs []string, claimant string, leaseTTL time.Duration) ([]*domain.ScheduledEvent, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	if claimant == "" {
		return nil, fmt.Errorf("claim scheduled events: claimant is required")
	}
	if leaseTTL <= 0 {
		return nil, fmt.Errorf("claim scheduled events: lease TTL must be positive")
	}

	coll := r.collection("scheduled_events")
	now := time.Now().UTC()
	expiresAt := now.Add(leaseTTL)
	var events []*domain.ScheduledEvent
	for _, eventID := range eventIDs {
		var doc ScheduledDoc
		filter := bson.M{
			"event_id": eventID,
			"$or": []bson.M{
				{"status": "pending", "scheduled_at": bson.M{"$lte": now}},
				{"status": "claimed", "claim_expires_at": bson.M{"$lte": now}},
			},
		}
		err := coll.FindOneAndUpdate(ctx, filter,
			bson.M{"$set": bson.M{
				"status":           "claimed",
				"claimant":         claimant,
				"claimed_at":       now,
				"claim_expires_at": expiresAt,
			}},
			options.FindOneAndUpdate().SetReturnDocument(options.After),
		).Decode(&doc)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				continue
			}
			return nil, fmt.Errorf("claim scheduled event: %w", err)
		}
		events = append(events, doc.ToDomain())
	}
	return events, nil
}

func (r *ScheduledEventRepository) MarkReleased(ctx context.Context, eventID, claimant string, releasedAt time.Time) error {
	coll := r.collection("scheduled_events")
	result, err := coll.UpdateOne(ctx,
		bson.M{"event_id": eventID, "status": "claimed", "claimant": claimant, "claim_expires_at": bson.M{"$gt": releasedAt}},
		bson.M{
			"$set": bson.M{"status": "released", "released_at": releasedAt},
			"$unset": bson.M{
				"claimant": "", "claimed_at": "", "claim_expires_at": "",
			},
		},
	)
	if err != nil {
		return fmt.Errorf("mark scheduled event released: %w", err)
	}
	if result.MatchedCount != 1 {
		return fmt.Errorf("mark scheduled event released: claim for event %q is no longer owned by %q", eventID, claimant)
	}
	return nil
}

func (r *ScheduledEventRepository) MarkFailed(ctx context.Context, eventID, claimant, errMsg string) error {
	coll := r.collection("scheduled_events")
	now := time.Now().UTC()
	result, err := coll.UpdateOne(ctx,
		bson.M{"event_id": eventID, "status": "claimed", "claimant": claimant, "claim_expires_at": bson.M{"$gt": now}},
		bson.M{
			"$set": bson.M{"status": "failed", "last_error": errMsg},
			"$unset": bson.M{
				"claimant": "", "claimed_at": "", "claim_expires_at": "",
			},
		},
	)
	if err != nil {
		return fmt.Errorf("mark scheduled event failed: %w", err)
	}
	if result.MatchedCount != 1 {
		return fmt.Errorf("mark scheduled event failed: claim for event %q is no longer owned by %q", eventID, claimant)
	}
	return nil
}

func (r *ScheduledEventRepository) Get(ctx context.Context, tenantID, eventID string) (*domain.ScheduledEvent, error) {
	coll := r.collection("scheduled_events")
	var doc ScheduledDoc
	err := coll.FindOne(ctx, bson.M{"tenant_id": tenantID, "event_id": eventID}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("get scheduled event: %w", err)
	}
	return doc.ToDomain(), nil
}

func (r *ScheduledEventRepository) List(ctx context.Context, tenantID string, limit, offset int) ([]*domain.ScheduledEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	coll := r.collection("scheduled_events")
	cursor, err := coll.Find(ctx,
		bson.M{"tenant_id": tenantID},
		options.Find().
			SetSort(bson.D{{Key: "scheduled_at", Value: -1}, {Key: "created_at", Value: -1}}).
			SetSkip(int64(offset)).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, fmt.Errorf("list scheduled events: %w", err)
	}
	defer cursor.Close(ctx)

	var events []*domain.ScheduledEvent
	for cursor.Next(ctx) {
		var doc ScheduledDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode scheduled event: %w", err)
		}
		events = append(events, doc.ToDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate scheduled events: %w", err)
	}
	return events, nil
}

func (r *ScheduledEventRepository) Cancel(ctx context.Context, tenantID, eventID string) error {
	coll := r.collection("scheduled_events")
	result, err := coll.UpdateOne(ctx,
		bson.M{"tenant_id": tenantID, "event_id": eventID, "status": "pending"},
		bson.M{"$set": bson.M{"status": "cancelled"}},
	)
	if err != nil {
		return fmt.Errorf("cancel scheduled event: %w", err)
	}
	if result.MatchedCount != 1 {
		return fmt.Errorf("cancel scheduled event: event %q not found or not pending for tenant %q", eventID, tenantID)
	}
	return nil
}

type ScheduledDoc struct {
	EventID        string            `bson:"event_id"`
	TenantID       string            `bson:"tenant_id"`
	EventType      string            `bson:"event_type"`
	PartitionKey   string            `bson:"partition_key"`
	WorkflowID     string            `bson:"workflow_id,omitempty"`
	Payload        []byte            `bson:"payload"`
	Headers        map[string]string `bson:"headers,omitempty"`
	ScheduledAt    time.Time         `bson:"scheduled_at"`
	Status         string            `bson:"status"`
	CreatedAt      time.Time         `bson:"created_at"`
	ReleasedAt     *time.Time        `bson:"released_at,omitempty"`
	Claimant       string            `bson:"claimant,omitempty"`
	ClaimedAt      *time.Time        `bson:"claimed_at,omitempty"`
	ClaimExpiresAt *time.Time        `bson:"claim_expires_at,omitempty"`
	LastError      string            `bson:"last_error,omitempty"`
}

func ScheduledDocFromDomain(event *domain.ScheduledEvent) *ScheduledDoc {
	return &ScheduledDoc{
		EventID:      event.EventID,
		TenantID:     event.TenantID,
		EventType:    event.EventType,
		PartitionKey: event.PartitionKey,
		WorkflowID:   event.WorkflowID,
		Payload:      append([]byte(nil), event.Payload...),
		Headers:      event.Headers,
		ScheduledAt:  event.ScheduledAt,
		Status:       event.Status,
		CreatedAt:    event.CreatedAt,
		ReleasedAt:   event.ReleasedAt,
	}
}

func (d *ScheduledDoc) ToDomain() *domain.ScheduledEvent {
	var payload json.RawMessage
	if d.Payload != nil {
		payload = json.RawMessage(append([]byte(nil), d.Payload...))
	}
	return &domain.ScheduledEvent{
		EventID:      d.EventID,
		TenantID:     d.TenantID,
		EventType:    d.EventType,
		PartitionKey: d.PartitionKey,
		WorkflowID:   d.WorkflowID,
		Payload:      payload,
		Headers:      d.Headers,
		ScheduledAt:  d.ScheduledAt,
		Status:       d.Status,
		CreatedAt:    d.CreatedAt,
		ReleasedAt:   d.ReleasedAt,
	}
}
