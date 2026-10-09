package workflow

import (
	"context"
	"fmt"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type Service struct {
	repo  ports.WorkflowRepository
	clock ports.Clock
}

func NewService(repo ports.WorkflowRepository, clock ports.Clock) *Service {
	return &Service{
		repo:  repo,
		clock: clock,
	}
}

func (s *Service) Create(ctx context.Context, workflow *domain.WorkflowInstance) error {
	if workflow.WorkflowID == "" {
		workflow.WorkflowID = domain.NewID()
	}
	if workflow.CreatedAt.IsZero() {
		workflow.CreatedAt = s.clock.Now()
	}
	workflow.UpdatedAt = s.clock.Now()
	workflow.Version = 1
	return s.repo.Save(ctx, workflow)
}

func (s *Service) Get(ctx context.Context, workflowID string) (*domain.WorkflowInstance, error) {
	return s.repo.Get(ctx, workflowID)
}

func (s *Service) UpdateState(ctx context.Context, workflowID, newState string, revision int64) error {
	workflow, err := s.repo.Get(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("get workflow: %w", err)
	}
	if workflow == nil {
		return domain.ErrWorkflowNotFound
	}

	if !workflow.CanTransition(workflow.State, newState) {
		return domain.ErrWorkflowInvalidTransition
	}

	if err := workflow.Transition(newState); err != nil {
		return err
	}
	workflow.CurrentRevision = revision
	workflow.UpdatedAt = s.clock.Now()
	if newState == domain.WorkflowStatusCompleted || newState == domain.WorkflowStatusFailed {
		now := s.clock.Now()
		workflow.CompletedAt = &now
	}

	return s.repo.Update(ctx, workflow)
}

func (s *Service) Transition(ctx context.Context, workflowID, fromState, toState string) error {
	workflow, err := s.repo.Get(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("get workflow: %w", err)
	}
	if workflow == nil {
		return domain.ErrWorkflowNotFound
	}

	if workflow.State != fromState {
		return fmt.Errorf("workflow in state %s, expected %s", workflow.State, fromState)
	}

	if !workflow.CanTransition(fromState, toState) {
		return domain.ErrWorkflowInvalidTransition
	}

	if err := workflow.Transition(toState); err != nil {
		return err
	}
	workflow.UpdatedAt = s.clock.Now()
	if toState == domain.WorkflowStatusCompleted || toState == domain.WorkflowStatusFailed {
		now := s.clock.Now()
		workflow.CompletedAt = &now
	}

	return s.repo.Update(ctx, workflow)
}

func (s *Service) GetByTenantAndState(ctx context.Context, tenantID, state string, limit int) ([]*domain.WorkflowInstance, error) {
	if limit <= 0 {
		limit = 100
	}
	return s.repo.GetByTenantAndState(ctx, tenantID, state, limit)
}

func (s *Service) GetByCorrelation(ctx context.Context, correlationID string) ([]*domain.WorkflowInstance, error) {
	return s.repo.GetByCorrelation(ctx, correlationID)
}

func (s *Service) ListDefinitions(ctx context.Context, tenantID string) ([]*domain.WorkflowDefinition, error) {
	// This requires a WorkflowDefinitionRepository - not available in base WorkflowRepository
	// The caller should use the definition repository directly
	return nil, fmt.Errorf("not implemented - use WorkflowDefinitionRepository directly")
}

func (s *Service) GetDefinition(ctx context.Context, tenantID, workflowType string, version int64) (*domain.WorkflowDefinition, error) {
	// This requires a WorkflowDefinitionRepository - not available in base WorkflowRepository
	// The caller should use the definition repository directly
	return nil, fmt.Errorf("not implemented - use WorkflowDefinitionRepository directly")
}

func (s *Service) SaveDefinition(ctx context.Context, def *domain.WorkflowDefinition) error {
	// This requires a WorkflowDefinitionRepository - not available in base WorkflowRepository
	// The caller should use the definition repository directly
	return fmt.Errorf("not implemented - use WorkflowDefinitionRepository directly")
}
