package application

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
	"github.com/flowrule/flowrule/internal/rules"
)

type ActivateRuleUseCase struct {
	compiler    *rules.Compiler
	ruleRepo    ports.RuleRepository
	activations ports.ActivationRepository
	clock       ports.Clock
}

func NewActivateRuleUseCase(
	compiler *rules.Compiler,
	ruleRepo ports.RuleRepository,
	activations ports.ActivationRepository,
	clock ports.Clock,
) *ActivateRuleUseCase {
	return &ActivateRuleUseCase{
		compiler:    compiler,
		ruleRepo:    ruleRepo,
		activations: activations,
		clock:       clock,
	}
}

func (uc *ActivateRuleUseCase) Execute(ctx context.Context, tenantScope string, ruleSet string, source json.RawMessage, actor string) (*domain.RuleActivation, error) {
	revision, err := uc.compiler.Compile(source)
	if err != nil {
		return nil, err
	}
	revision.RuleID = ruleSet

	if err := uc.ruleRepo.Save(ctx, tenantScope, revision); err != nil {
		return nil, fmt.Errorf("save revision: %w", err)
	}

	activation := &domain.RuleActivation{
		TenantScope: tenantScope,
		RuleSet:     ruleSet,
		Revision:    revision.Revision,
		Version:     1,
		Actor:       actor,
		ActivatedAt: uc.clock.Now(),
	}

	if err := uc.activations.Set(ctx, activation); err != nil {
		return nil, fmt.Errorf("set activation: %w", err)
	}

	return activation, nil
}
