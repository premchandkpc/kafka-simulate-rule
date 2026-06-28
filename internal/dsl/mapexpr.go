package dsl

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type MapExpr struct {
	FieldPath  []string
	ArrayIndex int
	ArrayField []string
	Construct  []MapKV
	IsArray    bool
}

type MapKV struct {
	Key       string
	FieldPath []string
}

// ParseMapExpr parses a jq-subset map expression into a MapExpr.
// Supported: .field, .field.nested, .field[], .field[N], {a:.x,b:.y}
func ParseMapExpr(input string) (*MapExpr, error) {
	input = strings.TrimSpace(input)
	if len(input) == 0 {
		return nil, fmt.Errorf("mapexpr: empty expression")
	}

	if input[0] == '{' {
		return parseConstruct(input)
	}
	if input[0] != '.' {
		return nil, fmt.Errorf("mapexpr: expression must start with '.' or '{', got %q", input[0])
	}

	path := strings.TrimPrefix(input, ".")
	if path == "" {
		return nil, fmt.Errorf("mapexpr: empty field path")
	}

	parts := strings.Split(path, ".")

	// Check last part for array operations
	last := parts[len(parts)-1]

	if strings.HasSuffix(last, "[]") {
		field := strings.TrimSuffix(last, "[]")
		expr := &MapExpr{
			IsArray: true,
		}
		if field == "" {
			// .items[] → ArrayField is ["items"]
			if len(parts) == 1 {
				expr.ArrayField = parts[:1]
				return expr, nil
			}
			// shouldn't happen with split logic above
			return nil, fmt.Errorf("mapexpr: invalid array expression")
		}
		// .items[].name  — not supported in our subset
		if len(parts) > 1 {
			return nil, fmt.Errorf("mapexpr: nested array access not supported")
		}
		expr.ArrayField = []string{field}
		return expr, nil
	}

	if strings.HasSuffix(last, "]") {
		open := strings.Index(last, "[")
		if open < 0 {
			return nil, fmt.Errorf("mapexpr: invalid array index syntax %q", last)
		}
		field := last[:open]
		idxStr := last[open+1 : len(last)-1]
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			return nil, fmt.Errorf("mapexpr: invalid array index %q: %w", idxStr, err)
		}
		if idx < 0 {
			return nil, fmt.Errorf("mapexpr: negative array index %d", idx)
		}

		if len(parts) > 1 {
			return nil, fmt.Errorf("mapexpr: nested array access not supported")
		}

		expr := &MapExpr{
			ArrayIndex: idx,
			IsArray:    true,
		}
		if field != "" {
			expr.ArrayField = []string{field}
		}
		return expr, nil
	}

	// Simple field path: .field.nested → ["field", "nested"]
	return &MapExpr{FieldPath: parts}, nil
}

func parseConstruct(input string) (*MapExpr, error) {
	inner := strings.TrimSpace(input[1 : len(input)-1])
	if inner == "" {
		return nil, fmt.Errorf("mapexpr: empty object construct")
	}

	expr := &MapExpr{}
	pairs := strings.Split(inner, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		colon := strings.Index(pair, ":")
		if colon < 0 {
			return nil, fmt.Errorf("mapexpr: object construct missing colon in %q", pair)
		}
		key := strings.TrimSpace(pair[:colon])
		val := strings.TrimSpace(pair[colon+1:])
		if key == "" || val == "" {
			return nil, fmt.Errorf("mapexpr: empty key or value in construct")
		}
		if val[0] != '.' {
			return nil, fmt.Errorf("mapexpr: construct value must be a field path starting with '.', got %q", val)
		}
		fieldPath := strings.Split(val[1:], ".")
		expr.Construct = append(expr.Construct, MapKV{Key: key, FieldPath: fieldPath})
	}
	return expr, nil
}

// EvalMapExpr evaluates a MapExpr against a JSON body and returns the result.
func EvalMapExpr(expr *MapExpr, body []byte) ([]byte, error) {
	switch {
	case expr.IsArray && expr.ArrayIndex >= 0:
		return extractArrayIndex(body, expr.ArrayField, expr.ArrayIndex)
	case len(expr.FieldPath) > 0:
		return extractPath(body, expr.FieldPath)
	case expr.IsArray && len(expr.ArrayField) > 0:
		return extractPath(body, expr.ArrayField)
	case len(expr.Construct) > 0:
		return constructObject(body, expr.Construct)
	default:
		return nil, fmt.Errorf("mapexpr: empty expression")
	}
}

func extractPath(body []byte, path []string) ([]byte, error) {
	var data any
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("mapexpr: unmarshal: %w", err)
	}

	current := data
	for _, part := range path {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("mapexpr: field %q not found in non-object", part)
		}
		val, ok := m[part]
		if !ok {
			return nil, fmt.Errorf("mapexpr: field %q not found", part)
		}
		current = val
	}

	return json.Marshal(current)
}

func extractArrayIndex(body []byte, fieldPath []string, idx int) ([]byte, error) {
	var data any
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("mapexpr: unmarshal: %w", err)
	}

	current := data
	for _, part := range fieldPath {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("mapexpr: field %q not found in non-object", part)
		}
		val, ok := m[part]
		if !ok {
			return nil, fmt.Errorf("mapexpr: field %q not found", part)
		}
		current = val
	}

	arr, ok := current.([]any)
	if !ok {
		return nil, fmt.Errorf("mapexpr: field is not an array")
	}
	if idx >= len(arr) {
		return nil, fmt.Errorf("mapexpr: array index %d out of bounds (len=%d)", idx, len(arr))
	}

	return json.Marshal(arr[idx])
}

func constructObject(body []byte, kvs []MapKV) ([]byte, error) {
	var data any
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("mapexpr: unmarshal: %w", err)
	}

	out := make(map[string]any, len(kvs))
	for _, kv := range kvs {
		current := data
		var found bool
		for _, part := range kv.FieldPath {
			m, ok := current.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("mapexpr: field %q not found", part)
			}
			val, ok := m[part]
			if !ok {
				return nil, fmt.Errorf("mapexpr: field %q not found", part)
			}
			current = val
			found = true
		}
		if found {
			out[kv.Key] = current
		}
	}

	return json.Marshal(out)
}
