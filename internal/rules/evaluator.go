package rules

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

var _ ports.RuleEvaluator = (*Evaluator)(nil)

type Evaluator struct{}

func NewEvaluator() *Evaluator {
	return &Evaluator{}
}

func (e *Evaluator) Evaluate(revision *domain.RuleRevision, event *domain.EventEnvelope, facts map[string]json.RawMessage) (*domain.Decision, error) {
	matched := make([]string, 0)
	effects := make([]domain.Effect, 0)
	explanations := make([]domain.RuleExplanation, 0, len(revision.Compiled))

	for _, rule := range revision.Compiled {
		ok, err := e.evalPredicate(rule.When, event.Data, facts)
		if err != nil {
			return nil, fmt.Errorf("rule %s: %w", rule.ID, err)
		}

		explanation := domain.RuleExplanation{
			RuleID:   rule.ID,
			RuleName: rule.Name,
			Matched:  ok,
			Priority: rule.Priority,
		}

		if ok {
			matched = append(matched, rule.ID)
			actions := make([]string, 0, len(rule.Then))
			for i, action := range rule.Then {
				effect := e.buildEffect(revision, event, rule.ID, i, action)
				effects = append(effects, effect)
				actions = append(actions, describeAction(action))
			}
			explanation.Reason = "predicate matched"
			explanation.Actions = actions
			if revision.MatchMode == domain.MatchModeFirstMatch {
				explanations = append(explanations, explanation)
				break
			}
		} else {
			actions := make([]string, 0, len(rule.Otherwise))
			for i, action := range rule.Otherwise {
				effect := e.buildEffect(revision, event, rule.ID, i, action)
				effects = append(effects, effect)
				actions = append(actions, describeAction(action))
			}
			explanation.Reason = "predicate did not match"
			explanation.Actions = actions
		}
		explanations = append(explanations, explanation)
	}

	hash := domain.ComputeDecisionHash(revision.ContentHash, event.ID, "", matched, effects)

	return &domain.Decision{
		MatchedRules: matched,
		Effects:      effects,
		Hash:         hash,
		Explanations: explanations,
	}, nil
}

func (e *Evaluator) evalPredicate(p domain.Predicate, data json.RawMessage, facts map[string]json.RawMessage) (bool, error) {
	if p.All != nil {
		for _, sub := range p.All {
			if sub == nil {
				return false, domain.ErrInvalidPredicate
			}
			ok, err := e.evalPredicate(*sub, data, facts)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
		}
		return true, nil
	}

	if p.Any != nil {
		for _, sub := range p.Any {
			if sub == nil {
				continue
			}
			ok, err := e.evalPredicate(*sub, data, facts)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	}

	if p.Not != nil {
		if p.Not == nil {
			return false, domain.ErrInvalidPredicate
		}
		ok, err := e.evalPredicate(*p.Not, data, facts)
		if err != nil {
			return false, err
		}
		return !ok, nil
	}

	return e.evalLeaf(p, data, facts)
}

func (e *Evaluator) evalLeaf(p domain.Predicate, data json.RawMessage, facts map[string]json.RawMessage) (bool, error) {
	val, err := resolvePath(p.Path, data, facts)
	if err != nil {
		if p.Op == domain.OpExists {
			return false, nil
		}
		if p.Op == domain.OpNotExists {
			return true, nil
		}
		return false, nil
	}

	if p.Op == domain.OpExists {
		return val != nil, nil
	}
	if p.Op == domain.OpNotExists {
		return val == nil, nil
	}

	if val == nil {
		return false, nil
	}

	switch p.Op {
	case domain.OpEq:
		return compareEq(val, p.Value), nil
	case domain.OpNeq:
		return !compareEq(val, p.Value), nil
	case domain.OpGt:
		return compareCmp(val, p.Value) > 0, nil
	case domain.OpGte:
		return compareCmp(val, p.Value) >= 0, nil
	case domain.OpLt:
		return compareCmp(val, p.Value) < 0, nil
	case domain.OpLte:
		return compareCmp(val, p.Value) <= 0, nil
	case domain.OpIn:
		return compareIn(val, p.Value), nil
	case domain.OpNotIn:
		return !compareIn(val, p.Value), nil
	default:
		return false, domain.ErrInvalidOperator
	}
}

func resolvePath(path string, data json.RawMessage, facts map[string]json.RawMessage) (interface{}, error) {
	if path == "" {
		return nil, domain.ErrInvalidFactPath
	}

	if !strings.HasPrefix(path, "$.") {
		return nil, domain.ErrInvalidFactPath
	}

	parts := strings.Split(path[2:], ".")
	var current interface{}

	if err := json.Unmarshal(data, &current); err != nil {
		return nil, fmt.Errorf("unmarshal data: %w", err)
	}

	for _, part := range parts {
		if part == "" {
			continue
		}
		switch v := current.(type) {
		case map[string]interface{}:
			val, ok := v[part]
			if !ok {
				return nil, nil
			}
			current = val
		default:
			return nil, nil
		}
	}

	return current, nil
}

func compareEq(actual interface{}, expected interface{}) bool {
	a := normalizeNumber(actual)
	b := normalizeNumber(expected)
	if a != nil && b != nil {
		return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
	}
	return fmt.Sprintf("%v", actual) == fmt.Sprintf("%v", expected)
}

func normalizeNumber(v interface{}) interface{} {
	switch n := v.(type) {
	case float64:
		if n == float64(int64(n)) {
			return int64(n)
		}
		return n
	case int64:
		return n
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i
		}
		if f, err := n.Float64(); err == nil {
			return f
		}
	}
	return v
}

func compareCmp(actual interface{}, expected interface{}) int {
	a, aOk := toFloat64(actual)
	b, bOk := toFloat64(expected)
	if aOk && bOk {
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	}
	sa := fmt.Sprintf("%v", actual)
	sb := fmt.Sprintf("%v", expected)
	return strings.Compare(sa, sb)
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	}
	return 0, false
}

func compareIn(val interface{}, list interface{}) bool {
	arr, ok := list.([]interface{})
	if !ok {
		return false
	}
	for _, item := range arr {
		if compareEq(val, item) {
			return true
		}
	}
	return false
}

func (e *Evaluator) buildEffect(revision *domain.RuleRevision, event *domain.EventEnvelope, ruleID string, actionIndex int, action domain.Action) domain.Effect {
	effectType := domain.EffectTypeEmit
	var dest, name string
	var payload json.RawMessage

	if action.Emit != nil {
		dest = action.Emit.Topic
		name = action.Emit.Topic
		payload = action.Emit.Data
	} else if action.Command != nil {
		effectType = domain.EffectTypeCommand
		dest = action.Command.Destination
		name = action.Command.Name
		payload = action.Command.Data
	}

	effectID := domain.ComputeEffectID(event.TenantID, event.ID, revision.RuleID, revision.Revision, ruleID, actionIndex)

	return domain.Effect{
		ID:          effectID,
		ExecutionID: "",
		Destination: dest,
		Name:        name,
		Payload:     payload,
		EffectType:  effectType,
		CreatedAt:   domain.SystemClock{}.Now(),
	}
}

func describeAction(action domain.Action) string {
	if action.Emit != nil {
		return "emit:" + action.Emit.Topic
	}
	if action.Command != nil {
		return "command:" + action.Command.Destination + "/" + action.Command.Name
	}
	return "unknown"
}