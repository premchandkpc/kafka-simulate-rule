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

type EventRepository struct {
	querier Querier
}

func NewEventRepository(q Querier) *EventRepository {
	return &EventRepository{querier: q}
}

func (r *EventRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

func (r *EventRepository) Save(ctx context.Context, event *domain.EventEnvelope) error {
	coll := r.collection("events")
	doc := EventDocFromDomain(event)
	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil
		}
		return fmt.Errorf("save event: %w", err)
	}
	return nil
}

func (r *EventRepository) Get(ctx context.Context, tenantID, eventID string) (*domain.EventEnvelope, error) {
	coll := r.collection("events")
	var doc EventDoc
	err := coll.FindOne(ctx, bson.M{
		"tenant_id": tenantID,
		"event_id":  eventID,
	}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("get event: %w", err)
	}
	return doc.ToDomain(), nil
}

func (r *EventRepository) Exists(ctx context.Context, tenantID, eventID string) (bool, error) {
	coll := r.collection("events")
	count, err := coll.CountDocuments(ctx, bson.M{
		"tenant_id": tenantID,
		"event_id":  eventID,
	}, options.Count().SetLimit(1))
	if err != nil {
		return false, fmt.Errorf("check event exists: %w", err)
	}
	return count > 0, nil
}

func (r *EventRepository) MarkProcessed(ctx context.Context, tenantID, eventID, executionID string) error {
	coll := r.collection("events")
	now := time.Now().UTC()
	_, err := coll.UpdateOne(ctx,
		bson.M{
			"tenant_id": tenantID,
			"event_id":  eventID,
		},
		bson.M{
			"$set": bson.M{
				"processed":    true,
				"execution_id": executionID,
				"processed_at": now,
			},
		},
	)
	return err
}

func (r *EventRepository) GetUnprocessed(ctx context.Context, tenantID string, limit int) ([]*domain.EventEnvelope, error) {
	coll := r.collection("events")

	filter := bson.M{
		"tenant_id": tenantID,
		"processed": bson.M{"$ne": true},
	}

	cursor, err := coll.Find(ctx, filter, options.Find().
		SetLimit(int64(limit)).
		SetSort(bson.D{{Key: "occurred_at", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("get unprocessed events: %w", err)
	}
	defer cursor.Close(ctx)

	var events []*domain.EventEnvelope
	for cursor.Next(ctx) {
		var doc EventDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode event: %w", err)
		}
		events = append(events, doc.ToDomain())
	}

	return events, nil
}

type EventDoc struct {
	EventID      string     `bson:"event_id"`
	TenantID     string     `bson:"tenant_id"`
	Type         string     `bson:"type"`
	PartitionKey string     `bson:"partition_key"`
	WorkflowID   string     `bson:"workflow_id,omitempty"`
	OccurredAt   time.Time  `bson:"occurred_at"`
	ScheduledAt  *time.Time `bson:"scheduled_at,omitempty"`
	Data         bson.Raw   `bson:"data"`
	Headers      bson.Raw   `bson:"headers,omitempty"`
	Processed    bool       `bson:"processed"`
	ExecutionID  string     `bson:"execution_id,omitempty"`
	ProcessedAt  *time.Time `bson:"processed_at,omitempty"`
	CreatedAt    time.Time  `bson:"created_at"`
}

func EventDocFromDomain(event *domain.EventEnvelope) *EventDoc {
	var data, headers bson.Raw
	if event.Data != nil {
		data, _ = bson.Marshal(event.Data)
	}
	if event.Headers != nil {
		headers, _ = bson.Marshal(event.Headers)
	}

	return &EventDoc{
		EventID:      event.ID,
		TenantID:     event.TenantID,
		Type:         event.Type,
		PartitionKey: event.PartitionKey,
		WorkflowID:   event.WorkflowID,
		OccurredAt:   event.OccurredAt,
		Data:         data,
		Headers:      headers,
		Processed:    false,
		CreatedAt:    time.Now().UTC(),
	}
}

func (d *EventDoc) ToDomain() *domain.EventEnvelope {
	var data json.RawMessage
	var headers map[string]string
	if d.Data != nil {
		data = json.RawMessage(d.Data)
	}
	if d.Headers != nil {
		json.Unmarshal(d.Headers, &headers)
	}

	return &domain.EventEnvelope{
		ID:           d.EventID,
		Type:         d.Type,
		TenantID:     d.TenantID,
		PartitionKey: d.PartitionKey,
		WorkflowID:   d.WorkflowID,
		OccurredAt:   d.OccurredAt,
		Data:         data,
		Headers:      headers,
	}
}
