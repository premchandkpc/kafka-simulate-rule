package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/flowrule/flowrule/internal/domain"
)

type ContractRegistry struct {
	mu      sync.RWMutex
	schemas map[string]*domain.ContractSchema
}

func NewContractRegistry() *ContractRegistry {
	return &ContractRegistry{
		schemas: make(map[string]*domain.ContractSchema),
	}
}

func (r *ContractRegistry) key(name, version string) string {
	return name + ":" + version
}

func (r *ContractRegistry) Get(ctx context.Context, name string, version string) (*domain.ContractSchema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	schema, ok := r.schemas[r.key(name, version)]
	if !ok {
		return nil, fmt.Errorf("contract %s@%s not found", name, version)
	}
	return schema, nil
}

func (r *ContractRegistry) Register(ctx context.Context, schema *domain.ContractSchema) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemas[r.key(schema.Name, schema.Version)] = schema
	return nil
}

func (r *ContractRegistry) List(ctx context.Context, name string) ([]domain.ContractSchema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []domain.ContractSchema
	for _, s := range r.schemas {
		if s.Name == name {
			result = append(result, *s)
		}
	}
	return result, nil
}
