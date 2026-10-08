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

type BatchRepository struct {
	querier Querier
}

func NewBatchRepository(querier Querier) *BatchRepository {
	return &BatchRepository{db: db}
}

func (r *BatchRepository) ListUnbatched(ctx context.Context, limit int) ([]*domain.InboxEntry, error) {
	coll := r.collection("inbox")
	
	if limit <= 0 {
		limit = 400
	}
	
	filter := bson.M{
		"status":     "committed",
		"batch_id":    bson.M{"$exists": false},
	}
	
	cursor, err := coll.Find(ctx, filter, options.Find().
		SetLimit(int64(limit)).
		SetSort(bson.D{{Key: "first_seen_at", Value: 1}, {Key: "tenant_id", Value: 1}, {Key: "event_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("list unbatched: %w", err)
	}
	defer cursor.Close(ctx)
	
	var entries []*domain.InboxEntry
	for cursor.Next(ctx) {
		var doc InboxDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode unbatched: %w", err)
		}
		entries = append(entries, doc.ToDomain())
	}
	
	return entries, nil
}

func (r *BatchRepository) MarkBatched(ctx context.Context, tenantID, eventID, batchID string) error {
	coll := r.collection("inbox")
	
	_, err := coll.UpdateOne(ctx,
		bson.M{
			"tenant_id": tenantID,
			"event_id":  eventID,
			"batch_id":  bson.M{"$exists": false},
		},
		bson.M{
			"$set": bson.M{
				"batch_id": batchID,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("mark batched: %w", err)
	}
	return nil
}

func (r *BatchRepository) SaveBatchRun(ctx context.Context, run *domain.BatchRun) error {
	coll := r.collection("batch_runs")
	doc := BatchRunDocFromDomain(run)
	
	_, err := coll.UpdateOne(ctx,
		bson.M{"batch_id": run.BatchID},
		bson.M{
			"$set": doc,
			"$setOnInsert": bson.M{
				"batch_id":    run.BatchID,
				"created_at":  run.CreatedAt,
			},
		},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("save batch run: %w", err)
	}
	return nil
}

func (r *BatchRepository) GetBatchRun(ctx context.Context, batchID string) (*domain.BatchRun, error) {
	coll := r.collection("batch_runs")
	var doc BatchRunDoc
	err := coll.FindOne(ctx, bson.M{"batch_id": batchID}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("get batch run: %w", err)
	}
	return doc.ToDomain(), nil
}

type BatchRunDoc struct {
	BatchID      string     `bson:"batch_id"`
	TenantID     string     `bson:"tenant_id"`
	PartitionKey string     `bson:"partition_key"`
	RuleSet      string     `bson:"rule_set"`
	Status       string     `bson:"status"`
	MemberCount  int        `bson:"member_count"`
	ExecutionID  string     `bson:"execution_id,omitempty"`
	DecisionHash string     `bson:"decision_hash,omitempty"`
	WindowFrom   *time.Time `bson:"window_from,omitempty"`
	WindowTo     *time.Time `bson:"window_to,omitempty"`
	CreatedAt    time.Time  `bson:"created_at"`
}

func BatchRunDocFromDomain(run *domain.BatchRun) *BatchRunDoc {
	return &BatchRunDoc{
		BatchID:      run.BatchID,
		TenantID:     run.TenantID,
		PartitionKey: run.PartitionKey,
		RuleSet:      run.RuleSet,
		Status:       run.Status,
		MemberCount:  run.MemberCount,
		ExecutionID:  run.ExecutionID,
		DecisionHash: run.DecisionHash,
		WindowFrom:   run.WindowFrom,
		WindowTo:     run.WindowTo,
		CreatedAt:    run.CreatedAt,
	}
}

func (d *BatchRunDoc) ToDomain() *domain.BatchRun {
	return &domain.BatchRun{
		BatchID:      d.BatchID,
		TenantID:     d.TenantID,
		PartitionKey: d.PartitionKey,
		RuleSet:      d.RuleSet,
		Status:       d.Status,
		MemberCount:  d.MemberCount,
		ExecutionID:  d.ExecutionID,
		DecisionHash: d.DecisionHash,
		WindowFrom:   d.WindowFrom,
		WindowTo:     d.WindowTo,
		CreatedAt:    d.CreatedAt,
	}
}