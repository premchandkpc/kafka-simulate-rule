package rules

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

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
				effect, err := e.buildEffect(revision, event, rule.ID, i, action)
				if err != nil {
					return nil, fmt.Errorf("rule %s action %d: %w", rule.ID, i, err)
				}
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
				effect, err := e.buildEffect(revision, event, rule.ID, i, action)
				if err != nil {
					return nil, fmt.Errorf("rule %s otherwise action %d: %w", rule.ID, i, err)
				}
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
		cmp := compareCmp(val, p.Value)
		if cmp == 2 {
			return false, fmt.Errorf("cannot compare non-numeric values for gt: %v vs %v", val, p.Value)
		}
		return cmp > 0, nil
	case domain.OpGte:
		cmp := compareCmp(val, p.Value)
		if cmp == 2 {
			return false, fmt.Errorf("cannot compare non-numeric values for gte: %v vs %v", val, p.Value)
		}
		return cmp >= 0, nil
	case domain.OpLt:
		cmp := compareCmp(val, p.Value)
		if cmp == 2 {
			return false, fmt.Errorf("cannot compare non-numeric values for lt: %v vs %v", val, p.Value)
		}
		return cmp < 0, nil
	case domain.OpLte:
		cmp := compareCmp(val, p.Value)
		if cmp == 2 {
			return false, fmt.Errorf("cannot compare non-numeric values for lte: %v vs %v", val, p.Value)
		}
		return cmp <= 0, nil
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

	// If first part is "facts", resolve from facts map
	if len(parts) > 0 && parts[0] == "facts" {
		if len(parts) < 2 {
			return nil, nil
		}
		factKey := parts[1]
		factData, ok := facts[factKey]
		if !ok {
			return nil, nil
		}
		if err := json.Unmarshal(factData, &current); err != nil {
			return nil, fmt.Errorf("unmarshal fact %s: %w", factKey, err)
		}
		// Process remaining parts
		for _, part := range parts[2:] {
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

	// Otherwise resolve from event data
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
	// If either value cannot be converted to a number, return an error indicator
	// by returning a special value that will cause the comparison to fail
	// This prevents incorrect lexical comparison like "100" < "20"
	return 2 // Indicates incompatible types for numeric comparison
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

// interpolateTemplates replaces template placeholders in the JSON data with actual values
// Supports: {{new_id}}, {{now}}, {{$.path}}
func interpolateTemplates(data json.RawMessage, parentEvent *domain.EventEnvelope, childID string) (json.RawMessage, error) {
	var dataMap map[string]interface{}
	if err := json.Unmarshal(data, &dataMap); err != nil {
		return nil, err
	}

	interpolated := interpolateMap(dataMap, dataMap, nil, childID)
	result, err := json.Marshal(interpolated)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(result), nil
}

func interpolateMap(m map[string]interface{}, root map[string]interface{}, parentEvent *domain.EventEnvelope, childID string) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		result[k] = interpolateValue(v, m, nil, childID)
	}
	return result
}

func interpolateValue(v interface{}, m map[string]interface{}, parentEvent *domain.EventEnvelope, childID string) interface{} {
	switch val := v.(type) {
	case string:
		return interpolateString(val, m, nil, childID)
	case map[string]interface{}:
		return interpolateMap(val, m, nil, childID)
	case []interface{}:
		result := make([]interface{}, len(val))
		for i, item := range val {
			result[i] = interpolateValue(item, m, nil, childID)
		}
		return result
	default:
		return val
	}
}

func interpolateString(s string, root map[string]interface{}, parentEvent *domain.EventEnvelope, childID string) string {
	// Replace {{new_id}}
	if strings.Contains(s, "{{new_id}}") {
		s = strings.ReplaceAll(s, "{{new_id}}", childID)
	}
	// Replace {{now}}
	if strings.Contains(s, "{{now}}") {
		s = strings.ReplaceAll(s, "{{now}}", time.Now().UTC().Format(time.RFC3339))
	}
	// Replace {{$.path}}
	if strings.Contains(s, "{{$.") && strings.HasSuffix(s, "}}") {
		key := strings.TrimPrefix(s, "{{$.")
		key = strings.TrimSuffix(key, "}}")
		if val, ok := root[key]; ok {
			return fmt.Sprintf("%v", val)
		}
		return ""
	}
	// Replace {{field}}
	if strings.HasPrefix(s, "{{") && strings.HasSuffix(s, "}}") {
		key := strings.TrimPrefix(s, "{{")
		key = strings.TrimSuffix(key, "}}")
		if val, ok := root[key]; ok {
			return fmt.Sprintf("%v", val)
		}
		return ""
	}
	return s
}

func (e *Evaluator) buildEffect(revision *domain.RuleRevision, event *domain.EventEnvelope, ruleID string, actionIndex int, action domain.Action) (domain.Effect, error) {
	effectType := domain.EffectTypeEmit
	var dest, name string
	var payload json.RawMessage

	if action.Emit != nil {
		dest = action.Emit.Topic
		name = action.Emit.Topic
		payload = action.Emit.Data
	} else if action.EmitEvent != nil {
		childID := domain.ComputeChildEventID(event.ID, ruleID, actionIndex)

		// Resolve partition key based on policy
		partitionKey := action.EmitEvent.PartitionKey
		policy := action.EmitEvent.PartitionKeyPolicy
		if policy == "" {
			policy = domain.PartitionKeyPolicyExplicit
		}

		switch policy {
		case domain.PartitionKeyPolicyInherit:
			partitionKey = event.PartitionKey
		case domain.PartitionKeyPolicyFromData:
			if action.EmitEvent.PartitionKeyPath != "" {
				val, err := resolvePath(action.EmitEvent.PartitionKeyPath, event.Data, nil)
				if err != nil || val == nil {
					return domain.Effect{}, fmt.Errorf("resolve partition key from path %s: %w", action.EmitEvent.PartitionKeyPath, err)
				}
				partitionKey = fmt.Sprintf("%v", val)
			}
		}

		if partitionKey == "" {
			return domain.Effect{}, fmt.Errorf("partition key resolution resulted in empty value for policy %s", policy)
		}

		// Interpolate template values in the effect data
		interpolatedData, err := interpolateTemplates(action.EmitEvent.Data, event, childID)
		if err != nil {
			return domain.Effect{}, fmt.Errorf("interpolate templates: %w", err)
		}

		child := domain.EventEnvelope{
			ID:           childID,
			Type:         action.EmitEvent.Type,
			TenantID:     event.TenantID,
			PartitionKey: partitionKey,
			OccurredAt:   domain.SystemClock{}.Now(),
			Data:         interpolatedData,
			Headers: map[string]string{
				"flowrule_parent_event_id": event.ID,
				"flowrule_child_event_id":  childID,
			},
		}
		encoded, err := json.Marshal(child)
		if err != nil {
			return domain.Effect{}, fmt.Errorf("marshal child event: %w", err)
		}
		effectType = domain.EffectTypeEmitEvent
		dest = "events." + child.Type
		name = child.Type
		payload = encoded
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
	}, nil
}

func describeAction(action domain.Action) string {
	if action.Emit != nil {
		return "emit:" + action.Emit.Topic
	}
	if action.EmitEvent != nil {
		return "emit_event:" + action.EmitEvent.Type
	}
	if action.Command != nil {
		return "command:" + action.Command.Destination + "/" + action.Command.Name
	}
	return "unknown"
}
