package rules

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/flowrule/flowrule/internal/domain"
)

const (
	CompilerVersion = "1.0.0"
)

type Limits struct {
	MaxRulesPerSet   int
	MaxPredicates    int
	MaxNestingDepth  int
	MaxActionsPerSet int
	MaxPayloadBytes  int
	MaxRuleIDLen     int
}

func DefaultLimits() Limits {
	return Limits{
		MaxRulesPerSet:   100,
		MaxPredicates:    200,
		MaxNestingDepth:  10,
		MaxActionsPerSet: 10,
		MaxPayloadBytes:  256 * 1024,
		MaxRuleIDLen:     128,
	}
}

type Compiler struct {
	limits Limits
}

func NewCompiler(limits Limits) *Compiler {
	return &Compiler{limits: limits}
}

type rawRuleSet struct {
	RuleSet  string          `json:"rule_set"`
	Revision int64           `json:"revision"`
	Mode     string          `json:"mode"`
	Rules    json.RawMessage `json:"rules"`
}

type rawRule struct {
	ID        string         `json:"id"`
	Priority  int            `json:"priority"`
	When      domain.Predicate `json:"when"`
	Then      []domain.Action  `json:"then"`
	Otherwise []domain.Action  `json:"otherwise,omitempty"`
}

func (c *Compiler) Compile(source json.RawMessage) (*domain.RuleRevision, error) {
	if len(source) == 0 {
		return nil, fmt.Errorf("empty rule source")
	}

	var rs rawRuleSet
	if err := json.Unmarshal(source, &rs); err != nil {
		return nil, fmt.Errorf("parse rule set: %w", err)
	}

	if rs.RuleSet == "" {
		return nil, domain.ErrRuleIDRequired
	}
	if rs.Revision <= 0 {
		return nil, domain.ErrRevisionRequired
	}
	if rs.Mode != "first_match" && rs.Mode != "all_matches" {
		return nil, domain.ErrInvalidMatchMode
	}

	var rawRules []rawRule
	if err := json.Unmarshal(rs.Rules, &rawRules); err != nil {
		return nil, fmt.Errorf("parse rules: %w", err)
	}
	if len(rawRules) == 0 {
		return nil, domain.ErrRuleExceedsLimits
	}
	if len(rawRules) > c.limits.MaxRulesPerSet {
		return nil, domain.ErrRuleExceedsLimits
	}

	compiled, err := c.compileRules(rawRules)
	if err != nil {
		return nil, err
	}

	revision := &domain.RuleRevision{
		RuleID:          rs.RuleSet,
		Revision:        rs.Revision,
		ContentHash:     domain.ComputeSourceHash(source),
		MatchMode:       domain.MatchMode(rs.Mode),
		Compiled:        compiled,
		Source:          source,
		CompilerVersion: CompilerVersion,
		CreatedAt:       domain.EventNow(),
	}
	return revision, nil
}

func (c *Compiler) compileRules(rawRules []rawRule) ([]domain.CompiledRule, error) {
	seenIDs := make(map[string]bool)
	seenPriorities := make(map[int]bool)
	compiled := make([]domain.CompiledRule, 0, len(rawRules))

	for _, r := range rawRules {
		if r.ID == "" {
			return nil, domain.ErrRuleIDRequired
		}
		if len(r.ID) > c.limits.MaxRuleIDLen {
			return nil, domain.ErrRuleExceedsLimits
		}
		if seenIDs[r.ID] {
			return nil, domain.ErrDuplicateRuleID
		}
		seenIDs[r.ID] = true

		if seenPriorities[r.Priority] {
			return nil, domain.ErrAmbiguousPriority
		}
		seenPriorities[r.Priority] = true

		if err := c.validatePredicate(r.When, 0); err != nil {
			return nil, err
		}

		predCount := countPredicates(r.When)
		if predCount > c.limits.MaxPredicates {
			return nil, domain.ErrRuleExceedsLimits
		}

		if len(r.Then) == 0 || len(r.Then) > c.limits.MaxActionsPerSet {
			return nil, domain.ErrRuleExceedsLimits
		}
		for _, a := range r.Then {
			if err := c.validateAction(a); err != nil {
				return nil, err
			}
		}
		if len(r.Otherwise) > c.limits.MaxActionsPerSet {
			return nil, domain.ErrRuleExceedsLimits
		}
		for _, a := range r.Otherwise {
			if err := c.validateAction(a); err != nil {
				return nil, err
			}
		}

		compiled = append(compiled, domain.CompiledRule{
			ID:        r.ID,
			Priority:  r.Priority,
			When:      r.When,
			Then:      r.Then,
			Otherwise: r.Otherwise,
		})
	}

	sort.Slice(compiled, func(i, j int) bool {
		return compiled[i].Priority > compiled[j].Priority
	})

	return compiled, nil
}

func (c *Compiler) validatePredicate(p domain.Predicate, depth int) error {
	if depth > c.limits.MaxNestingDepth {
		return domain.ErrRuleExceedsLimits
	}

	hasLeaf := p.Path != ""
	hasComposite := p.All != nil || p.Any != nil || p.Not != nil
	if hasLeaf && hasComposite {
		return domain.ErrInvalidPredicate
	}
	if !hasLeaf && !hasComposite {
		return domain.ErrInvalidPredicate
	}

	if hasLeaf {
		if !domain.ValidOp(p.Op) {
			return domain.ErrInvalidOperator
		}
		return nil
	}

	if p.All != nil {
		for _, sub := range p.All {
			if sub == nil {
				return domain.ErrInvalidPredicate
			}
			if err := c.validatePredicate(*sub, depth+1); err != nil {
				return err
			}
		}
	}
	if p.Any != nil {
		for _, sub := range p.Any {
			if sub == nil {
				return domain.ErrInvalidPredicate
			}
			if err := c.validatePredicate(*sub, depth+1); err != nil {
				return err
			}
		}
	}
	if p.Not != nil {
		if err := c.validatePredicate(*p.Not, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func (c *Compiler) validateAction(a domain.Action) error {
	hasEmit := a.Emit != nil
	hasCommand := a.Command != nil
	if hasEmit && hasCommand {
		return domain.ErrInvalidAction
	}
	if !hasEmit && !hasCommand {
		return domain.ErrInvalidAction
	}
	if hasEmit {
		if a.Emit.Topic == "" {
			return domain.ErrInvalidAction
		}
		data, _ := json.Marshal(a.Emit.Data)
		if len(data) > c.limits.MaxPayloadBytes {
			return domain.ErrRuleExceedsLimits
		}
	}
	if hasCommand {
		if a.Command.Destination == "" {
			return domain.ErrInvalidAction
		}
		if a.Command.Name == "" {
			return domain.ErrInvalidAction
		}
		data, _ := json.Marshal(a.Command.Data)
		if len(data) > c.limits.MaxPayloadBytes {
			return domain.ErrRuleExceedsLimits
		}
	}
	return nil
}

func countPredicates(p domain.Predicate) int {
	count := 0
	if p.All != nil {
		for _, sub := range p.All {
			if sub != nil {
				count += countPredicates(*sub)
			}
		}
	}
	if p.Any != nil {
		for _, sub := range p.Any {
			if sub != nil {
				count += countPredicates(*sub)
			}
		}
	}
	if p.Not != nil {
		count += countPredicates(*p.Not)
	}
	if p.Path != "" {
		count++
	}
	return count
}

func SourceHash(source json.RawMessage) string {
	return string(domain.ComputeSourceHash(source))
}

func FilterActiveByType(compiled []domain.CompiledRule, eventType string) []domain.CompiledRule {
	return compiled
}

func SortByPriority(rules []domain.CompiledRule) {
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].Priority > rules[j].Priority
	})
}

func RuleIDs(rules []domain.CompiledRule) []string {
	ids := make([]string, len(rules))
	for i, r := range rules {
		ids[i] = r.ID
	}
	return ids
}

func DescribePredicate(p domain.Predicate) string {
	if p.Path != "" {
		return fmt.Sprintf("%s %s %v", p.Path, p.Op, p.Value)
	}
	parts := []string{}
	if p.All != nil {
		subParts := make([]string, 0, len(p.All))
		for _, sub := range p.All {
			if sub != nil {
				subParts = append(subParts, DescribePredicate(*sub))
			}
		}
		parts = append(parts, "all("+strings.Join(subParts, ", ")+")")
	}
	if p.Any != nil {
		subParts := make([]string, 0, len(p.Any))
		for _, sub := range p.Any {
			if sub != nil {
				subParts = append(subParts, DescribePredicate(*sub))
			}
		}
		parts = append(parts, "any("+strings.Join(subParts, ", ")+")")
	}
	if p.Not != nil {
		parts = append(parts, "not("+DescribePredicate(*p.Not)+")")
	}
	return strings.Join(parts, " ")
}