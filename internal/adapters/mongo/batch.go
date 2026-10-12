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

type BatchRepository struct {
	querier Querier
}

func NewBatchRepository(querier Querier) *BatchRepository {
	return &BatchRepository{querier: querier}
}

func (r *BatchRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

func (r *BatchRepository) ListUnbatched(ctx context.Context, limit int) ([]*domain.InboxEntry, error) {
	coll := r.collection(collectionInbox)

	if limit <= 0 {
		limit = 400
	}

	filter := bson.M{
		"status":   "committed",
		"batch_id": bson.M{"$exists": false},
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
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate unbatched: %w", err)
	}

	return entries, nil
}

// ListUnbatchedByIDs returns only requested eligible events in one tenant partition.
func (r *BatchRepository) ListUnbatchedByIDs(ctx context.Context, tenantID, partitionKey, ruleSet string, eventIDs []string) ([]*domain.InboxEntry, error) {
	if len(eventIDs) == 0 {
		return []*domain.InboxEntry{}, nil
	}
	filter := bson.M{
		"tenant_id":     tenantID,
		"partition_key": partitionKey,
		"rule_set":      ruleSet,
		"event_id":      bson.M{"$in": eventIDs},
		"status":        "committed",
		"batch_id":      bson.M{"$exists": false},
	}
	cursor, err := r.collection(collectionInbox).Find(ctx, filter, options.Find().
		SetSort(bson.D{{Key: "first_seen_at", Value: 1}, {Key: "tenant_id", Value: 1}, {Key: "event_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("list requested unbatched events: %w", err)
	}
	defer cursor.Close(ctx)

	var entries []*domain.InboxEntry
	for cursor.Next(ctx) {
		var doc InboxDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode requested unbatched event: %w", err)
		}
		entries = append(entries, doc.ToDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate requested unbatched events: %w", err)
	}
	return entries, nil
}


 // CompleteBatch atomically persists the completed run and all member associations
 // using a MongoDB transaction. The deployment must support replica-set transactions.
func (r *BatchRepository) CompleteBatch(ctx context.Context, run *domain.BatchRun, eventIDs []string) error {
	if run == nil || run.BatchID == "" || run.TenantID == "" || run.PartitionKey == "" || run.RuleSet == "" {
		return fmt.Errorf("batch run identity is required")
	}
	if len(eventIDs) == 0 || len(eventIDs) != run.MemberCount {
		return fmt.Errorf("batch member count does not match run")
	}
	seen := make(map[string]struct{}, len(eventIDs))
	for _, id := range eventIDs {
		if id == "" {
			return fmt.Errorf("batch event ID must not be empty")
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("duplicate batch event ID %q", id)
		}
		seen[id] = struct{}{}
	}

	client := r.collection(collectionInbox).Database().Client()
	session, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("start batch completion session: %w", err)
	}
	defer session.EndSession(context.Background())

	_, err = session.WithTransaction(ctx, func(sc mongo.SessionContext) (interface{}, error) {
		runs := r.collection(collectionBatchRuns)
		doc := BatchRunDocFromDomain(run)
		_, err := runs.UpdateOne(sc,
			bson.M{
				"batch_id": run.BatchID,
				"tenant_id": run.TenantID,
				"partition_key": run.PartitionKey,
				"rule_set": run.RuleSet,
			},
			bson.M{"$set": doc},
			options.Update().SetUpsert(true),
		)
		if err != nil {
			return nil, fmt.Errorf("upsert batch run: %w", err)
		}

		filter := bson.M{
			"tenant_id": run.TenantID,
			"partition_key": run.PartitionKey,
			"rule_set": run.RuleSet,
			"event_id": bson.M{"$in": eventIDs},
			"status": "committed",
			"$or": []bson.M{
				{"batch_id": bson.M{"$exists": false}},
				{"batch_id": run.BatchID},
			},
		}
		result, err := r.collection(collectionInbox).UpdateMany(sc, filter,
			bson.M{"$set": bson.M{"batch_id": run.BatchID}},
		)
		if err != nil {
			return nil, fmt.Errorf("associate batch members: %w", err)
		}
		if result.MatchedCount != int64(len(eventIDs)) {
			return nil, fmt.Errorf("batch membership conflict: matched %d of %d requested events", result.MatchedCount, len(eventIDs))
		}
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("complete batch transaction: %w", err)
	}
	return nil
}

func (r *BatchRepository) MarkBatched(ctx context.Context, tenantID, eventID, batchID string) error {
	coll := r.collection(collectionInbox)

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

func (r *BatchRepository) SaveRun(ctx context.Context, run *domain.BatchRun) error {
	coll := r.collection(collectionBatchRuns)
	doc := BatchRunDocFromDomain(run)

	_, err := coll.UpdateOne(ctx,
		bson.M{"batch_id": run.BatchID},
		bson.M{"$set": doc},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("save batch run: %w", err)
	}
	return nil
}

func (r *BatchRepository) GetRun(ctx context.Context, batchID string) (*domain.BatchRun, error) {
	coll := r.collection(collectionBatchRuns)
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

func (r *BatchRepository) GetRunForTenant(ctx context.Context, tenantID, batchID string) (*domain.BatchRun, error) {
	coll := r.collection(collectionBatchRuns)
	var doc BatchRunDoc
	err := coll.FindOne(ctx, bson.M{"tenant_id": tenantID, "batch_id": batchID}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("get batch run for tenant: %w", err)
	}
	return doc.ToDomain(), nil
}

func (r *BatchRepository) ListRuns(ctx context.Context, tenantID, ruleSet string, limit, offset int) ([]*domain.BatchRun, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	filter := bson.M{"tenant_id": tenantID}
	if ruleSet != "" {
		filter["rule_set"] = ruleSet
	}

	// lgtm[go/sql-injection] Filter values are BSON scalars under fixed keys; no query syntax is interpolated.
	cursor, err := r.collection(collectionBatchRuns).Find(ctx, filter, options.Find().
		SetSort(bson.D{{Key: "created_at", Value: -1}, {Key: "batch_id", Value: -1}}).
		SetSkip(int64(offset)).
		SetLimit(int64(limit)))
	if err != nil {
		return nil, fmt.Errorf("list batch runs: %w", err)
	}
	defer cursor.Close(ctx)
	var runs []*domain.BatchRun
	for cursor.Next(ctx) {
		var doc BatchRunDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode batch run: %w", err)
		}
		runs = append(runs, doc.ToDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate batch runs: %w", err)
	}
	return runs, nil
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
