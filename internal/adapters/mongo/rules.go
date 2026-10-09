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

type Repository struct {
	querier Querier
}

func NewRepository(q Querier) *Repository {
	return &Repository{querier: q}
}

func (r *Repository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

func (r *Repository) GetActive(ctx context.Context, tenantScope string, ruleSet string) (*domain.RuleRevision, error) {
	coll := r.collection("rule_revisions")

	var activation ActivationDoc
	err := r.collection("rule_activations").FindOne(ctx, bson.M{
		"tenant_scope": tenantScope,
		"rule_set":     ruleSet,
	}).Decode(&activation)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("get activation: %w", err)
	}

	var revision RevisionDoc
	err = coll.FindOne(ctx, bson.M{
		"tenant_scope": tenantScope,
		"rule_id":      activation.RuleSet,
		"revision":     activation.Revision,
	}).Decode(&revision)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("get revision: %w", err)
	}

	return revision.ToDomain(), nil
}

func (r *Repository) Get(ctx context.Context, tenantScope string, ruleSet string, revisionNum int64) (*domain.RuleRevision, error) {
	coll := r.collection("rule_revisions")

	var revision RevisionDoc
	err := coll.FindOne(ctx, bson.M{
		"tenant_scope": tenantScope,
		"rule_id":      ruleSet,
		"revision":     revisionNum,
	}).Decode(&revision)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("get revision: %w", err)
	}

	return revision.ToDomain(), nil
}

func (r *Repository) Save(ctx context.Context, revision *domain.RuleRevision) error {
	coll := r.collection("rule_revisions")

	doc := RevisionDocFromDomain(revision.TenantScope, revision)
	_, err := coll.InsertOne(ctx, doc)
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
}

func (r *Repository) List(ctx context.Context, tenantScope string) ([]*domain.RuleRevision, error) {
	coll := r.collection("rule_revisions")
	cursor, err := coll.Find(ctx, bson.M{"tenant_scope": tenantScope})
	if err != nil {
		return nil, fmt.Errorf("list revisions: %w", err)
	}
	defer cursor.Close(ctx)

	var revisions []*domain.RuleRevision
	for cursor.Next(ctx) {
		var doc RevisionDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode revision: %w", err)
		}
		revisions = append(revisions, doc.ToDomain())
	}
	return revisions, nil
}

func (r *Repository) Delete(ctx context.Context, tenantScope string, ruleSet string, revision int64) error {
	coll := r.collection("rule_revisions")
	_, err := coll.DeleteOne(ctx, bson.M{
		"tenant_scope": tenantScope,
		"rule_id":      ruleSet,
		"revision":     revision,
	})
	if err != nil {
		return fmt.Errorf("delete revision: %w", err)
	}
	return nil
}

type ActivationDoc struct {
	TenantScope string    `bson:"tenant_scope"`
	RuleSet     string    `bson:"rule_set"`
	Revision    int64     `bson:"revision"`
	Version     int64     `bson:"version"`
	Actor       string    `bson:"actor"`
	ActivatedAt time.Time `bson:"activated_at"`
}

type RevisionDoc struct {
	TenantScope     string              `bson:"tenant_scope"`
	RuleID          string              `bson:"rule_id"`
	Revision        int64               `bson:"revision"`
	Source          bson.Raw            `bson:"source"`
	Compiled        bson.Raw            `bson:"compiled"`
	ContentHash     string              `bson:"content_hash"`
	CompilerVersion string              `bson:"compiler_version"`
	MatchMode       string              `bson:"match_mode"`
	InputContract   *domain.ContractRef `bson:"input_contract,omitempty"`
	CreatedAt       time.Time           `bson:"created_at"`
}

func (d *RevisionDoc) ToDomain() *domain.RuleRevision {
	var compiled []domain.CompiledRule
	_ = bson.Unmarshal(d.Compiled, &compiled)

	return &domain.RuleRevision{
		TenantScope:     d.TenantScope,
		RuleID:          d.RuleID,
		Revision:        d.Revision,
		Source:          json.RawMessage(d.Source),
		Compiled:        compiled,
		ContentHash:     d.ContentHash,
		CompilerVersion: d.CompilerVersion,
		MatchMode:       domain.MatchMode(d.MatchMode),
		InputContract:   d.InputContract,
		CreatedAt:       d.CreatedAt,
	}
}

func RevisionDocFromDomain(tenantScope string, revision *domain.RuleRevision) *RevisionDoc {
	compiled, _ := bson.Marshal(revision.Compiled)
	source, _ := bson.Marshal(revision.Source)

	return &RevisionDoc{
		TenantScope:     tenantScope,
		RuleID:          revision.RuleID,
		Revision:        revision.Revision,
		Source:          source,
		Compiled:        compiled,
		ContentHash:     revision.ContentHash,
		CompilerVersion: revision.CompilerVersion,
		MatchMode:       string(revision.MatchMode),
		InputContract:   revision.InputContract,
		CreatedAt:       revision.CreatedAt,
	}
}

type ActivationRepository struct {
	querier Querier
}

func NewActivationRepository(q Querier) *ActivationRepository {
	return &ActivationRepository{querier: q}
}

func (r *ActivationRepository) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

func (r *ActivationRepository) Get(ctx context.Context, tenantScope string, ruleSet string) (*domain.RuleActivation, error) {
	var doc ActivationDoc
	err := r.collection("rule_activations").FindOne(ctx, bson.M{
		"tenant_scope": tenantScope,
		"rule_set":     ruleSet,
	}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("get activation: %w", err)
	}
	return doc.ToDomain(), nil
}

func (r *ActivationRepository) Set(ctx context.Context, activation *domain.RuleActivation) error {
	coll := r.collection("rule_activations")
	filter := bson.M{"tenant_scope": activation.TenantScope, "rule_set": activation.RuleSet}
	update := bson.M{
		"$set": bson.M{
			"revision":     activation.Revision,
			"version":      activation.Version,
			"actor":        activation.Actor,
			"activated_at": activation.ActivatedAt,
		},
		"$setOnInsert": bson.M{
			"tenant_scope": activation.TenantScope,
			"rule_set":     activation.RuleSet,
		},
	}
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)
	var result ActivationDoc
	err := coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)
	return err
}

func (d *ActivationDoc) ToDomain() *domain.RuleActivation {
	return &domain.RuleActivation{
		TenantScope: d.TenantScope,
		RuleSet:     d.RuleSet,
		Revision:    d.Revision,
		Version:     d.Version,
		Actor:       d.Actor,
		ActivatedAt: d.ActivatedAt,
	}
}

func (r *ActivationRepository) Delete(ctx context.Context, tenantScope string, ruleSet string) error {
	coll := r.collection("rule_activations")
	_, err := coll.DeleteOne(ctx, bson.M{
		"tenant_scope": tenantScope,
		"rule_set":     ruleSet,
	})
	if err != nil {
		return fmt.Errorf("delete activation: %w", err)
	}
	return nil
}
