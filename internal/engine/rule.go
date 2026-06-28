package engine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/premchand/flowrule/internal/dsl"
)

// ConditionFunc evaluates a message body against a match condition.
// It must not allocate on the hot path.
type ConditionFunc func(body []byte) bool

// MatchConfig describes a rule match condition from YAML.
type MatchConfig struct {
	Field string `yaml:"field"`
	Op    string `yaml:"op"`
	Value string `yaml:"value"`
}

// IdempotencyConfig controls deduplication.
type IdempotencyConfig struct {
	Enabled   bool   `yaml:"enabled"`
	KeyField  string `yaml:"key_field"`
	TTLSecond int    `yaml:"ttl_seconds"`
}

// OrderingConfig controls key-based ordering.
type OrderingConfig struct {
	Enabled  bool   `yaml:"enabled"`
	KeyField string `yaml:"key_field"`
}

// CompiledRule is a parsed and compiled rule, ready for matching and execution.
type CompiledRule struct {
	ID          string
	Priority    int
	Matcher     ConditionFunc
	Plan        *dsl.ExecutionPlan
	Idempotency *IdempotencyConfig
	Ordering    *OrderingConfig
}

// buildConditionFunc compiles a MatchConfig into a ConditionFunc.
func buildConditionFunc(mc MatchConfig) (ConditionFunc, error) {
	if mc.Field == "*" {
		return func([]byte) bool { return true }, nil
	}
	switch mc.Op {
	case "eq":
		return buildEqualCondition(mc.Field, mc.Value), nil
	case "gt":
		return buildGreaterCondition(mc.Field, mc.Value)
	case "lt":
		return buildLessCondition(mc.Field, mc.Value)
	case "contains":
		return buildContainsCondition(mc.Field, mc.Value), nil
	case "*":
		return func([]byte) bool { return true }, nil
	default:
		return nil, fmt.Errorf("engine: unknown match operator %q", mc.Op)
	}
}

func buildEqualCondition(field, value string) ConditionFunc {
	return func(body []byte) bool {
		var data map[string]any
		if err := json.Unmarshal(body, &data); err != nil {
			return false
		}
		v, ok := data[field]
		if !ok {
			return false
		}
		return fmt.Sprint(v) == value
	}
}

func buildGreaterCondition(field, value string) (ConditionFunc, error) {
	return func(body []byte) bool {
		var data map[string]any
		if err := json.Unmarshal(body, &data); err != nil {
			return false
		}
		v, ok := data[field]
		if !ok {
			return false
		}
		s := fmt.Sprint(v)
		return strings.Compare(s, value) > 0
	}, nil
}

func buildLessCondition(field, value string) (ConditionFunc, error) {
	return func(body []byte) bool {
		var data map[string]any
		if err := json.Unmarshal(body, &data); err != nil {
			return false
		}
		v, ok := data[field]
		if !ok {
			return false
		}
		s := fmt.Sprint(v)
		return strings.Compare(s, value) < 0
	}, nil
}

func buildContainsCondition(field, value string) ConditionFunc {
	return func(body []byte) bool {
		var data map[string]any
		if err := json.Unmarshal(body, &data); err != nil {
			return false
		}
		v, ok := data[field]
		if !ok {
			return false
		}
		return strings.Contains(fmt.Sprint(v), value)
	}
}
