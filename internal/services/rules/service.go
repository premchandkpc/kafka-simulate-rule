package rules

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

// Service handles rule compilation, activation, and querying.
type Service struct {
	compiler    ports.RuleCompiler
	ruleRepo    ports.RuleRepository
	activations ports.ActivationRepository
	executions  ports.ExecutionRepository
	clock       ports.Clock
}

func NewService(
	compiler ports.RuleCompiler,
	ruleRepo ports.RuleRepository,
	activations ports.ActivationRepository,
	executions ports.ExecutionRepository,
	clock ports.Clock,
) *Service {
	return &Service{
		compiler:    compiler,
		ruleRepo:    ruleRepo,
		activations: activations,
		executions:  executions,
		clock:       clock,
	}
}

// Activate compiles and activates a rule revision.
// The tenantScope should be derived from authenticated user context.
func (s *Service) Activate(ctx context.Context, tenantScope string, ruleSet string, source json.RawMessage, actor string) (*domain.RuleActivation, error) {
	revision, err := s.compiler.Compile(source)
	if err != nil {
		return nil, err
	}
	revision.RuleID = ruleSet
	revision.TenantScope = tenantScope

	if err := s.ruleRepo.Save(ctx, revision); err != nil {
		return nil, fmt.Errorf("save revision: %w", err)
	}

	// Get current activation version for optimistic concurrency
	currentActivation, err := s.activations.Get(ctx, tenantScope, ruleSet)
	version := int64(1)
	if currentActivation != nil {
		version = currentActivation.Version + 1
	}

	activation := &domain.RuleActivation{
		TenantScope:  tenantScope,
		RuleSet:      ruleSet,
		Revision:     revision.Revision,
		Version:      version,
		Actor:        actor,
		ActivatedAt:  s.clock.Now(),
	}

	if err := s.activations.Set(ctx, activation); err != nil {
		return nil, fmt.Errorf("set activation: %w", err)
	}

	return activation, nil
}

// GetExecution retrieves an execution by ID.
func (s *Service) GetExecution(ctx context.Context, executionID string) (*domain.Execution, error) {
	return s.executions.Get(ctx, executionID)
}
