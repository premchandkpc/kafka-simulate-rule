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
	txManager   ports.TransactionManager
}

func NewService(
	compiler ports.RuleCompiler,
	ruleRepo ports.RuleRepository,
	activations ports.ActivationRepository,
	executions ports.ExecutionRepository,
	clock ports.Clock,
	txManager ports.TransactionManager,
) *Service {
	return &Service{
		compiler:    compiler,
		ruleRepo:    ruleRepo,
		activations: activations,
		executions:  executions,
		clock:       clock,
		txManager:   txManager,
	}
}

// Activate compiles and activates a rule revision atomically with CAS.
// The tenantScope should be derived from authenticated user context.
func (s *Service) Activate(ctx context.Context, tenantScope string, ruleSet string, source json.RawMessage, actor string) (*domain.RuleActivation, error) {
	revision, err := s.compiler.Compile(source)
	if err != nil {
		return nil, err
	}
	revision.RuleID = ruleSet
	revision.TenantScope = tenantScope

	return s.activateWithCAS(ctx, tenantScope, ruleSet, revision, actor)
}

// activateWithCAS performs the activation in a transaction with optimistic concurrency control.
func (s *Service) activateWithCAS(ctx context.Context, tenantScope, ruleSet string, revision *domain.RuleRevision, actor string) (*domain.RuleActivation, error) {
	tx, err := s.txManager.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	repos, err := s.newTxRepos(tx)
	if err != nil {
		return nil, fmt.Errorf("create tx repos: %w", err)
	}
	if txCtx := tx.Context(); txCtx != nil {
		ctx = txCtx
	}

	// Save the immutable revision
	if err := repos.RuleRepo.Save(ctx, revision); err != nil {
		return nil, fmt.Errorf("save revision: %w", err)
	}

	// Get current activation for CAS
	currentActivation, err := repos.Activations.Get(ctx, tenantScope, ruleSet)
	if err != nil {
		return nil, fmt.Errorf("get activation: %w", err)
	}

	expectedVersion := int64(0)
	if currentActivation != nil {
		expectedVersion = currentActivation.Version
	}

	newVersion := expectedVersion + 1
	activation := &domain.RuleActivation{
		TenantScope: tenantScope,
		RuleSet:     ruleSet,
		Revision:    revision.Revision,
		Version:     newVersion,
		Actor:       actor,
		ActivatedAt: s.clock.Now(),
	}

	// CAS: only update if version matches expected
	if err := repos.Activations.SetWithVersion(ctx, activation, expectedVersion); err != nil {
		return nil, fmt.Errorf("set activation: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return activation, nil
}

// ruleTxRepos holds the repositories needed for rule activation.
type ruleTxRepos struct {
	RuleRepo    ports.RuleRepository
	Activations ports.ActivationRepository
}

func (s *Service) newTxRepos(tx ports.Transaction) (ruleTxRepos, error) {
	// We need access to rule repo and activations within a transaction
	// The actual implementation will depend on how the storage factory provides tx-scoped repos
	// For now, we use the global repos but they should work within the transaction context
	return ruleTxRepos{
		RuleRepo:    s.ruleRepo,
		Activations: s.activations,
	}, nil
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
