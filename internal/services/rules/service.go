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
	currentActivation, getErr := s.activations.Get(ctx, tenantScope, ruleSet)
	version := int64(1)
	if getErr != nil {
		return nil, fmt.Errorf("get activation: %w", getErr)
	}
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

// ListRevisions lists all rule revisions for a tenant scope.
func (s *Service) ListRevisions(ctx context.Context, tenantScope string) ([]*domain.RuleRevision, error) {
	return s.ruleRepo.List(ctx, tenantScope)
}

// GetRevision retrieves a specific rule revision.
func (s *Service) GetRevision(ctx context.Context, tenantScope, ruleSet string, revision int64) (*domain.RuleRevision, error) {
	return s.ruleRepo.Get(ctx, tenantScope, ruleSet, revision)
}

// Deactivate deactivates a rule set by removing its activation.
func (s *Service) Deactivate(ctx context.Context, tenantScope, ruleSet string) error {
	// Get current activation to verify it exists
	_, err := s.activations.Get(ctx, tenantScope, ruleSet)
	if err != nil {
		return fmt.Errorf("get activation: %w", err)
	}
	// In this design, deactivation means setting activation to nil/zero revision
	// The activation record is deleted
	return s.activations.Delete(ctx, tenantScope, ruleSet)
}

// ListActiveRules lists all currently active rule sets for a tenant scope.
func (s *Service) ListActiveRules(ctx context.Context, tenantScope string) ([]*domain.RuleActivation, error) {
	// This would require a new repository method. For now, we can list all rule revisions
	// and check which have active activations.
	// TODO: Add ListActivations to ActivationRepository
	revisions, err := s.ruleRepo.List(ctx, tenantScope)
	if err != nil {
		return nil, fmt.Errorf("list revisions: %w", err)
	}

	var active []*domain.RuleActivation
	for _, rev := range revisions {
		activation, err := s.activations.Get(ctx, tenantScope, rev.RuleID)
		if err == nil && activation != nil && activation.Revision == rev.Revision {
			active = append(active, activation)
		}
	}
	return active, nil
}
