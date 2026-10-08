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

type WorkflowRepository struct {
	querier Querier
}

func NewWorkflowRepository(q Querier) *WorkflowRepository {
	return &WorkflowRepository{querier: q}
}

func (r *WorkflowRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

func (r *WorkflowRepository) Save(ctx context.Context, workflow *domain.WorkflowInstance) error {
	coll := r.collection("workflow_instances")
	doc := WorkflowDocFromDomain(workflow)
	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil
		}
		return fmt.Errorf("save workflow: %w", err)
	}
	return nil
}

func (r *WorkflowRepository) Get(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error) {
	coll := r.collection("workflow_instances")
	var doc WorkflowDoc
	err := coll.FindOne(ctx, bson.M{
		"workflow_id": workflowID,
	}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("get workflow: %w", err)
	}
	return doc.ToDomain(), nil
}

func (r *WorkflowRepository) Update(ctx context.Context, workflow *domain.WorkflowInstance) error {
	coll := r.collection("workflow_instances")
	now := time.Now().UTC()
	
	filter := bson.M{
		"workflow_id": workflow.WorkflowID,
		"version":     workflow.Version - 1,
	}
	
	update := bson.M{
		"$set": bson.M{
			"state":            workflow.State,
			"current_revision": workflow.CurrentRevision,
			"context":          workflow.Context,
			"updated_at":       now,
			"completed_at":     workflow.CompletedAt,
		},
		"$inc": bson.M{
			"version": 1,
		},
	}
	
	result, err := coll.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update workflow: %w", err)
	}
	if result.MatchedCount == 0 {
		return domain.ErrWorkflowConflict
	}
	
	workflow.Version++
	workflow.UpdatedAt = now
	return nil
}

func (r *WorkflowRepository) GetByTenantAndState(ctx context.Context, tenantID, state string, limit int) ([]*domain.WorkflowInstance, error) {
	coll := r.collection("workflow_instances")
	
	filter := bson.M{
		"tenant_id": tenantID,
		"state":     state,
	}
	
	cursor, err := coll.Find(ctx, filter, options.Find().
		SetLimit(int64(limit)).
		SetSort(bson.D{{Key: "updated_at", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("get workflows by tenant and state: %w", err)
	}
	defer cursor.Close(ctx)
	
	var workflows []*domain.WorkflowInstance
	for cursor.Next(ctx) {
		var doc WorkflowDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode workflow: %w", err)
		}
		workflows = append(workflows, doc.ToDomain())
	}
	
	return workflows, nil
}

type WorkflowDoc struct {
	WorkflowID      string     `bson:"workflow_id"`
	TenantID        string     `bson:"tenant_id"`
	WorkflowType    string     `bson:"workflow_type"`
	State           string     `bson:"state"`
	Version         int64      `bson:"version"`
	CurrentRevision int64      `bson:"current_revision,omitempty"`
	Context         bson.Raw   `bson:"context,omitempty"`
	CreatedAt       time.Time  `bson:"created_at"`
	UpdatedAt       time.Time  `bson:"updated_at"`
	CompletedAt     *time.Time `bson:"completed_at,omitempty"`
}

func WorkflowDocFromDomain(w *domain.WorkflowInstance) *WorkflowDoc {
	return &WorkflowDoc{
		WorkflowID:      w.WorkflowID,
		TenantID:        w.TenantID,
		WorkflowType:    w.WorkflowType,
		State:           w.State,
		Version:         w.Version,
		CurrentRevision: w.CurrentRevision,
		Context:         w.Context,
		CreatedAt:       w.CreatedAt,
		UpdatedAt:       w.UpdatedAt,
		CompletedAt:     w.CompletedAt,
	}
}

func (d *WorkflowDoc) ToDomain() *domain.WorkflowInstance {
	return &domain.WorkflowInstance{
		WorkflowID:      d.WorkflowID,
		TenantID:        d.TenantID,
		WorkflowType:    d.WorkflowType,
		State:           d.State,
		Version:         d.Version,
		CurrentRevision: d.CurrentRevision,
		Context:         d.Context,
		CreatedAt:       d.CreatedAt,
		UpdatedAt:       d.UpdatedAt,
		CompletedAt:     d.CompletedAt,
	}
}

type WorkflowDefinitionRepository struct {
	querier Querier
}

func NewWorkflowDefinitionRepository(q Querier) *WorkflowDefinitionRepository {
	return &WorkflowDefinitionRepository{querier: q}
}

func (r *WorkflowDefinitionRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

func (r *WorkflowDefinitionRepository) Save(ctx context.Context, def *domain.WorkflowDefinition) error {
	coll := r.collection("workflow_definitions")
	doc := WorkflowDefinitionDocFromDomain(def)
	_, err := coll.UpdateOne(ctx,
		bson.M{"workflow_type": def.WorkflowType, "version": def.Version},
		bson.M{"$set": doc},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("save workflow definition: %w", err)
	}
	return nil
}

func (r *WorkflowDefinitionRepository) Get(ctx context.Context, workflowType string, version int64) (*domain.WorkflowDefinition, error) {
	coll := r.collection("workflow_definitions")
	var doc WorkflowDefinitionDoc
	err := coll.FindOne(ctx, bson.M{
		"workflow_type": workflowType,
		"version":       version,
	}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("get workflow definition: %w", err)
	}
	return doc.ToDomain(), nil
}

func (r *WorkflowDefinitionRepository) List(ctx context.Context, tenantID string) ([]*domain.WorkflowDefinition, error) {
	coll := r.collection("workflow_definitions")
	cursor, err := coll.Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "workflow_type", Value: 1}, {Key: "version", Value: -1}}))
	if err != nil {
		return nil, fmt.Errorf("list workflow definitions: %w", err)
	}
	defer cursor.Close(ctx)

	var defs []*domain.WorkflowDefinition
	for cursor.Next(ctx) {
		var doc WorkflowDefinitionDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode workflow definition: %w", err)
		}
		defs = append(defs, doc.ToDomain())
	}
	return defs, nil
}

func (r *WorkflowDefinitionRepository) Delete(ctx context.Context, workflowType string, version int64) error {
	coll := r.collection("workflow_definitions")
	_, err := coll.DeleteOne(ctx, bson.M{"workflow_type": workflowType, "version": version})
	if err != nil {
		return fmt.Errorf("delete workflow definition: %w", err)
	}
	return nil
}

type WorkflowDefinitionDoc struct {
	WorkflowType string                 `bson:"workflow_type"`
	Version      int64                  `bson:"version"`
	States       []domain.WorkflowState `bson:"states"`
	Transitions  []domain.WorkflowTransition `bson:"transitions"`
	Rules        map[string]string      `bson:"rules,omitempty"`
	CreatedAt    time.Time              `bson:"created_at"`
	UpdatedAt    time.Time              `bson:"updated_at"`
}

func WorkflowDefinitionDocFromDomain(d *domain.WorkflowDefinition) *WorkflowDefinitionDoc {
	return &WorkflowDefinitionDoc{
		WorkflowType: d.WorkflowType,
		Version:      d.Version,
		States:       d.States,
		Transitions:  d.Transitions,
		Rules:        d.Rules,
		CreatedAt:    d.CreatedAt,
		UpdatedAt:    d.UpdatedAt,
	}
}

func (d *WorkflowDefinitionDoc) ToDomain() *domain.WorkflowDefinition {
	return &domain.WorkflowDefinition{
		WorkflowType: d.WorkflowType,
		Version:      d.Version,
		States:       d.States,
		Transitions:  d.Transitions,
		Rules:        d.Rules,
		CreatedAt:    d.CreatedAt,
		UpdatedAt:    d.UpdatedAt,
	}
}