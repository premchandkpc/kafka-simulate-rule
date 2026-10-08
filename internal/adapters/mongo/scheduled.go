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
	coll := r.collection("scheduled_events")
	
	filter := bson.M{
		"status":        "pending",
		"scheduled_at":  bson.M{"$lte": before},
	}
	
	cursor, err := coll.Find(ctx, filter, options.Find().SetLimit(int64(limit)))
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
	
	return events, nil
}

func (r *ScheduledEventRepository) MarkReleased(ctx context.Context, eventID string, releasedAt time.Time) error {
	coll := r.collection("scheduled_events")
	
	_, err := coll.UpdateOne(ctx,
		bson.M{
			"event_id": eventID,
			"status":   "pending",
		},
		bson.M{
			"$set": bson.M{
				"status":       "released",
				"released_at":  releasedAt,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("mark released: %w", err)
	}
	return nil
}

func (r *ScheduledEventRepository) MarkFailed(ctx context.Context, eventID string, errMsg string) error {
	coll := r.collection("scheduled_events")
	
	_, err := coll.UpdateOne(ctx,
		bson.M{
			"event_id": eventID,
			"status":   "pending",
		},
		bson.M{
			"$set": bson.M{
				"status": "failed",
			},
		},
	)
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	return nil
}

func (r *ScheduledEventRepository) Get(ctx context.Context, eventID string) (*domain.ScheduledEvent, error) {
	coll := r.collection("scheduled_events")
	var doc ScheduledDoc
	err := coll.FindOne(ctx, bson.M{"event_id": eventID}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("get scheduled event: %w", err)
	}
	return doc.ToDomain(), nil
}

type ScheduledDoc struct {
	EventID       string     `bson:"event_id"`
	TenantID      string     `bson:"tenant_id"`
	EventType     string     `bson:"event_type"`
	PartitionKey  string     `bson:"partition_key"`
	WorkflowID    string     `bson:"workflow_id,omitempty"`
	Payload       bson.Raw   `bson:"payload"`
	Headers       bson.Raw   `bson:"headers,omitempty"`
	ScheduledAt   time.Time  `bson:"scheduled_at"`
	Status        string     `bson:"status"`
	CreatedAt     time.Time  `bson:"created_at"`
	ReleasedAt    *time.Time `bson:"released_at,omitempty"`
}

func ScheduledDocFromDomain(event *domain.ScheduledEvent) *ScheduledDoc {
	var payload, headers bson.Raw
	if event.Payload != nil {
		payload = bson.Raw(event.Payload)
	}
	if event.Headers != nil {
		headers, _ = bson.Marshal(event.Headers)
	}
	return &ScheduledDoc{
		EventID:      event.EventID,
		TenantID:     event.TenantID,
		EventType:    event.EventType,
		PartitionKey: event.PartitionKey,
		WorkflowID:   event.WorkflowID,
		Payload:      payload,
		Headers:      headers,
		ScheduledAt:  event.ScheduledAt,
		Status:       event.Status,
		CreatedAt:    event.CreatedAt,
		ReleasedAt:   event.ReleasedAt,
	}
}

func (d *ScheduledDoc) ToDomain() *domain.ScheduledEvent {
	var payload json.RawMessage
	var headers map[string]string
	if d.Payload != nil {
		payload = json.RawMessage(d.Payload)
	}
	if d.Headers != nil {
		bson.Unmarshal(d.Headers, &headers)
	}
	return &domain.ScheduledEvent{
		EventID:      d.EventID,
		TenantID:     d.TenantID,
		EventType:    d.EventType,
		PartitionKey: d.PartitionKey,
		WorkflowID:   d.WorkflowID,
		Payload:      payload,
		Headers:      headers,
		ScheduledAt:  d.ScheduledAt,
		Status:       d.Status,
		CreatedAt:    d.CreatedAt,
		ReleasedAt:   d.ReleasedAt,
	}
}