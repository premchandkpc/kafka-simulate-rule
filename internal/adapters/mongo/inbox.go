package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/flowrule/flowrule/internal/domain"
)

type InboxRepository struct {
	querier Querier
}

func NewInboxRepository(q Querier) *InboxRepository {
	return &InboxRepository{querier: q}
}

func (r *InboxRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

func (r *InboxRepository) Insert(ctx context.Context, entry *domain.InboxEntry) (bool, error) {
	coll := r.collection("inbox")
	doc := InboxDocFromDomain(entry)
	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		return false, fmt.Errorf("insert inbox: %w", err)
	}
	return true, nil
}

func (r *InboxRepository) Get(ctx context.Context, tenantID string, eventID string) (*domain.InboxEntry, error) {
	coll := r.collection("inbox")
	var doc InboxDoc
	err := coll.FindOne(ctx, bson.M{
		"tenant_id": tenantID,
		"event_id":  eventID,
	}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("get inbox: %w", err)
	}
	return doc.ToDomain(), nil
}

func (r *InboxRepository) MarkCommitted(ctx context.Context, tenantID string, eventID string, executionID string) error {
	coll := r.collection("inbox")
	now := time.Now().UTC()
	_, err := coll.UpdateOne(ctx,
		bson.M{
			"tenant_id": tenantID,
			"event_id":  eventID,
			"status":    domain.InboxStatusProcessing,
		},
		bson.M{
			"$set": bson.M{
				"status":        domain.InboxStatusCommitted,
				"execution_id":  executionID,
				"committed_at":  now,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("mark committed: %w", err)
	}
	return nil
}

type InboxDoc struct {
	TenantID     string    `bson:"tenant_id"`
	EventID      string    `bson:"event_id"`
	Status       string    `bson:"status"`
	ExecutionID  string    `bson:"execution_id,omitempty"`
	FirstSeenAt  time.Time `bson:"first_seen_at"`
	CommittedAt  *time.Time `bson:"committed_at,omitempty"`
	PartitionKey string    `bson:"partition_key,omitempty"`
	RuleSet      string    `bson:"rule_set,omitempty"`
	Payload      bson.Raw  `bson:"payload,omitempty"`
	BatchID      string    `bson:"batch_id,omitempty"`
}

func InboxDocFromDomain(entry *domain.InboxEntry) *InboxDoc {
	var payload bson.Raw
	if entry.Payload != nil {
		payload, _ = bson.Marshal(entry.Payload)
	}
	return &InboxDoc{
		TenantID:     entry.TenantID,
		EventID:      entry.EventID,
		Status:       string(entry.Status),
		ExecutionID:  entry.ExecutionID,
		FirstSeenAt:  entry.FirstSeenAt,
		CommittedAt:  entry.CommittedAt,
		PartitionKey: entry.PartitionKey,
		RuleSet:      entry.RuleSet,
		Payload:      payload,
		BatchID:      entry.BatchID,
	}
}

func (d *InboxDoc) ToDomain() *domain.InboxEntry {
	var payload json.RawMessage
	if d.Payload != nil {
		payload = json.RawMessage(d.Payload)
	}
	return &domain.InboxEntry{
		TenantID:     d.TenantID,
		EventID:      d.EventID,
		Status:       domain.InboxStatus(d.Status),
		ExecutionID:  d.ExecutionID,
		FirstSeenAt:  d.FirstSeenAt,
		CommittedAt:  d.CommittedAt,
		PartitionKey: d.PartitionKey,
		RuleSet:      d.RuleSet,
		Payload:      payload,
		BatchID:      d.BatchID,
	}
}