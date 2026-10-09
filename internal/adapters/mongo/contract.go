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

type ContractRegistry struct {
	querier Querier
}

func NewContractRegistry(q Querier) *ContractRegistry {
	return &ContractRegistry{querier: q}
}

func (r *ContractRegistry) collection(name string) *mongo.Collection {
	return r.querier.Collection(name)
}

func (r *ContractRegistry) Register(ctx context.Context, schema *domain.ContractSchema) error {
	coll := r.collection("contract_schemas")
	doc := ContractSchemaDocFromDomain(schema)
	_, err := coll.UpdateOne(ctx,
		bson.M{"name": schema.Name, "version": schema.Version},
		bson.M{"$set": doc},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("register contract: %w", err)
	}
	return nil
}

func (r *ContractRegistry) Get(ctx context.Context, name, version string) (*domain.ContractSchema, error) {
	coll := r.collection("contract_schemas")
	var doc ContractSchemaDoc
	err := coll.FindOne(ctx, bson.M{
		"name":    name,
		"version": version,
	}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("get contract: %w", err)
	}
	return doc.ToDomain(), nil
}

func (r *ContractRegistry) List(ctx context.Context, name string) ([]*domain.ContractSchema, error) {
	coll := r.collection("contract_schemas")
	filter := bson.M{}
	if name != "" {
		filter["name"] = name
	}
	cursor, err := coll.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "name", Value: 1}, {Key: "version", Value: -1}}))
	if err != nil {
		return nil, fmt.Errorf("list contracts: %w", err)
	}
	defer cursor.Close(ctx)

	var schemas []*domain.ContractSchema
	for cursor.Next(ctx) {
		var doc ContractSchemaDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode contract: %w", err)
		}
		schemas = append(schemas, doc.ToDomain())
	}
	return schemas, nil
}

func (r *ContractRegistry) Delete(ctx context.Context, name, version string) error {
	coll := r.collection("contract_schemas")
	_, err := coll.DeleteOne(ctx, bson.M{"name": name, "version": version})
	if err != nil {
		return fmt.Errorf("delete contract: %w", err)
	}
	return nil
}

type ContractSchemaDoc struct {
	Name          string                          `bson:"name"`
	Version       string                          `bson:"version"`
	Namespace     string                          `bson:"namespace,omitempty"`
	Description   string                          `bson:"description,omitempty"`
	Fields        map[string]domain.ContractField `bson:"fields"`
	Compatibility string                          `bson:"compatibility,omitempty"`
	Owner         string                          `bson:"owner,omitempty"`
	Deprecated    bool                            `bson:"deprecated,omitempty"`
	CreatedAt     time.Time                       `bson:"created_at"`
	UpdatedAt     time.Time                       `bson:"updated_at"`
}

func ContractSchemaDocFromDomain(s *domain.ContractSchema) *ContractSchemaDoc {
	return &ContractSchemaDoc{
		Name:          s.Name,
		Version:       s.Version,
		Namespace:     s.Namespace,
		Description:   s.Description,
		Fields:        s.Fields,
		Compatibility: s.Compatibility,
		Owner:         s.Owner,
		Deprecated:    s.Deprecated,
		CreatedAt:     s.CreatedAt,
		UpdatedAt:     s.UpdatedAt,
	}
}

func (d *ContractSchemaDoc) ToDomain() *domain.ContractSchema {
	return &domain.ContractSchema{
		Name:          d.Name,
		Version:       d.Version,
		Namespace:     d.Namespace,
		Description:   d.Description,
		Fields:        d.Fields,
		Compatibility: d.Compatibility,
		Owner:         d.Owner,
		Deprecated:    d.Deprecated,
		CreatedAt:     d.CreatedAt,
		UpdatedAt:     d.UpdatedAt,
	}
}

var _ ports.ContractRegistry = (*ContractRegistry)(nil)
