package dsl

import (
	"testing"
)

func TestParseMapExpr(t *testing.T) {
	tests := []struct {
		input   string
		want    *MapExpr
		wantErr bool
	}{
		{
			input: ".result",
			want:  &MapExpr{FieldPath: []string{"result"}},
		},
		{
			input: ".data.payload",
			want:  &MapExpr{FieldPath: []string{"data", "payload"}},
		},
		{
			input:   "invalid",
			wantErr: true,
		},
		{
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseMapExpr(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseMapExpr(%q) expected error", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseMapExpr(%q) error: %v", tt.input, err)
			}
			if len(got.FieldPath) != len(tt.want.FieldPath) {
				t.Errorf("FieldPath len=%d, want %d", len(got.FieldPath), len(tt.want.FieldPath))
				return
			}
			for i, fp := range got.FieldPath {
				if fp != tt.want.FieldPath[i] {
					t.Errorf("FieldPath[%d] = %q, want %q", i, fp, tt.want.FieldPath[i])
				}
			}
		})
	}
}

func TestEvalMapExpr(t *testing.T) {
	body := []byte(`{"order_id":"ORD-123","amount":5000,"user":{"name":"Alice","tier":"gold"},"items":[{"sku":"A","qty":1},{"sku":"B","qty":2}]}`)

	tests := []struct {
		name     string
		expr     *MapExpr
		want     string
		wantErr  bool
	}{
		{
			name: "extract top-level field",
			expr: &MapExpr{FieldPath: []string{"order_id"}},
			want: `"ORD-123"`,
		},
		{
			name: "extract nested field",
			expr: &MapExpr{FieldPath: []string{"user", "name"}},
			want: `"Alice"`,
		},
		{
			name: "extract numeric field",
			expr: &MapExpr{FieldPath: []string{"amount"}},
			want: `5000`,
		},
		{
			name: "extract array element by index",
			expr: &MapExpr{ArrayField: []string{"items"}, ArrayIndex: 0, IsArray: true},
			want: `{"qty":1,"sku":"A"}`,
		},
		{
			name: "construct object",
			expr: &MapExpr{
				Construct: []MapKV{
					{Key: "id", FieldPath: []string{"order_id"}},
					{Key: "amt", FieldPath: []string{"amount"}},
				},
			},
			want: `{"amt":5000,"id":"ORD-123"}`,
		},
		{
			name:    "non-existent field",
			expr:    &MapExpr{FieldPath: []string{"nonexistent"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EvalMapExpr(tt.expr, body)
			if tt.wantErr {
				if err == nil {
					t.Errorf("EvalMapExpr expected error, got %s", string(got))
				}
				return
			}
			if err != nil {
				t.Fatalf("EvalMapExpr error: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("EvalMapExpr = %s, want %s", string(got), tt.want)
			}
		})
	}
}

func TestEvalMapExprConstructOrder(t *testing.T) {
	body := []byte(`{"a":1,"b":2}`)
	expr := &MapExpr{
		Construct: []MapKV{
			{Key: "x", FieldPath: []string{"a"}},
			{Key: "y", FieldPath: []string{"b"}},
		},
	}
	got, err := EvalMapExpr(expr, body)
	if err != nil {
		t.Fatalf("EvalMapExpr error: %v", err)
	}
	if string(got) != `{"x":1,"y":2}` {
		t.Errorf("EvalMapExpr = %s, want %s", string(got), `{"x":1,"y":2}`)
	}
}
