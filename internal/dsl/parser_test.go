package dsl

import (
	"testing"
)

func TestParseNext(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []Instruction
		wantErr bool
	}{
		{
			name:  "simple next",
			input: "n:validate",
			want: []Instruction{
				{Op: OpNext, Targets: []string{"validate"}},
			},
		},
		{
			name:    "next with comma fails",
			input:   "n:a,b",
			wantErr: true,
		},
		{
			name: "next with timeout",
			input: "t500 n:validate",
			want: []Instruction{
				{Op: OpNext, TimeoutMs: 500},
				{Op: OpNext, Targets: []string{"validate"}},
			},
		},
		{
			name:    "t zero fails",
			input:   "t0 n:validate",
			wantErr: true,
		},
		{
			name: "parallel then collect",
			input: "p:a,b c",
			want: []Instruction{
				{Op: OpParallel, Targets: []string{"a", "b"}},
				{Op: OpCollect},
			},
		},
		{
			name:    "p: single target fails",
			input:   "p:a c",
			wantErr: true,
		},
		{
			name:    "c without p: fails",
			input:   "c",
			wantErr: true,
		},
		{
			name: "fallback",
			input: "n:a f:b",
			want: []Instruction{
				{Op: OpNext, Targets: []string{"a"}},
				{Op: OpFallback, Targets: []string{"b"}},
			},
		},
		{
			name:    "f: with comma fails",
			input:   "f:a,b",
			wantErr: true,
		},
		{
			name: "gate with pipe",
			input: "g:amount>10000 n:a | n:b",
			want: []Instruction{
				{Op: OpGate, Operand: "amount", Operator: ">", Value: "10000"},
				{Op: OpNext, Targets: []string{"a"}},
				{Op: OpPipe},
				{Op: OpNext, Targets: []string{"b"}},
			},
		},
		{
			name:    "pipe without gate fails",
			input:   "n:a | n:b",
			wantErr: true,
		},
		{
			name:  "emit",
			input: "e:a,b,c",
			want: []Instruction{
				{Op: OpEmit, Targets: []string{"a", "b", "c"}},
			},
		},
		{
			name:  "drop",
			input: "d",
			want: []Instruction{
				{Op: OpDrop},
			},
		},
		{
			name:  "key",
			input: "k:user_id",
			want: []Instruction{
				{Op: OpKey, Operand: "user_id"},
			},
		},
		{
			name:  "split",
			input: "s:region",
			want: []Instruction{
				{Op: OpSplit, Operand: "region"},
			},
		},
		{
			name:  "map",
			input: "m:.result",
			want: []Instruction{
				{Op: OpMap, MapExpr: &MapExpr{FieldPath: []string{"result"}}},
			},
		},
		{
			name:  "buffer",
			input: "b5",
			want: []Instruction{
				{Op: OpBuffer, RetryN: 5},
			},
		},
		{
			name:    "buffer exceeds max",
			input:   "b99999",
			wantErr: true,
		},
		{
			name: "full pipeline",
			input: "t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics",
			want: []Instruction{
				{Op: OpNext, TimeoutMs: 500},
				{Op: OpNext, Targets: []string{"validate"}},
				{Op: OpNext, TimeoutMs: 1000},
				{Op: OpParallel, Targets: []string{"fraud", "inventory"}},
				{Op: OpCollect},
				{Op: OpFallback, Targets: []string{"dlq"}},
				{Op: OpNext, Targets: []string{"fulfill"}},
				{Op: OpEmit, Targets: []string{"notify", "analytics"}},
			},
		},
		{
			name:    "r after p: fails",
			input:   "p:a,b r3 c",
			wantErr: true,
		},
		{
			name: "r after n: works",
			input: "n:a r3",
			want: []Instruction{
				{Op: OpNext, Targets: []string{"a"}},
				{Op: OpNext, RetryN: 3},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens, err := Lex(tt.input)
			if err != nil {
				t.Fatalf("Lex(%q) unexpected error: %v", tt.input, err)
			}
			got, err := Parse(tokens)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Parse(%q) expected error, got %v instructions", tt.input, len(got))
				}
				return
			}
			if err != nil {
				t.Errorf("Parse(%q) unexpected error: %v", tt.input, err)
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("Parse(%q) len=%d, want %d\n  got:  %+v\n  want: %+v",
					tt.input, len(got), len(tt.want), got, tt.want)
				return
			}
			for i, instr := range got {
				if instr.Op != tt.want[i].Op {
					t.Errorf("Parse(%q)[%d].Op = %v, want %v", tt.input, i, instr.Op, tt.want[i].Op)
				}
				if len(instr.Targets) != len(tt.want[i].Targets) {
					t.Errorf("Parse(%q)[%d].Targets len=%d, want %d", tt.input, i, len(instr.Targets), len(tt.want[i].Targets))
				}
				if instr.Operand != tt.want[i].Operand {
					t.Errorf("Parse(%q)[%d].Operand = %q, want %q", tt.input, i, instr.Operand, tt.want[i].Operand)
				}
				if instr.TimeoutMs != tt.want[i].TimeoutMs {
					t.Errorf("Parse(%q)[%d].TimeoutMs = %d, want %d", tt.input, i, instr.TimeoutMs, tt.want[i].TimeoutMs)
				}
				if instr.RetryN != tt.want[i].RetryN {
					t.Errorf("Parse(%q)[%d].RetryN = %d, want %d", tt.input, i, instr.RetryN, tt.want[i].RetryN)
				}
			}
		})
	}
}

func TestParseGate(t *testing.T) {
	tests := []struct {
		input            string
		wantField, wantOp, wantVal string
		wantErr          bool
	}{
		{"g:amount>10000", "amount", ">", "10000", false},
		{"g:status==blocked", "status", "==", "blocked", false},
		{"g:user.tier!=gold", "user.tier", "!=", "gold", false},
		{"g:score>=95", "score", ">=", "95", false},
		{"g:tags.contains:vip", "tags", "contains", ":vip", false},
		{"g:amount<=0", "amount", "<=", "0", false},
		{"g:count<5", "count", "<", "5", false},
		{"g:a>b", "a", ">", "b", false},
		{"g:>10000", "", "", "", true},
		{"g:amount", "", "", "", true},
		{"g:", "", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			tokens, err := Lex(tt.input)
			if err != nil {
				if tt.wantErr {
					return
				}
				t.Fatalf("Lex error: %v", err)
			}
			if len(tokens) != 1 {
				t.Fatalf("expected 1 token, got %d", len(tokens))
			}
			instrs, err := Parse(tokens)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Parse(%q) expected error", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tt.input, err)
			}
			if len(instrs) != 1 {
				t.Fatalf("expected 1 instruction, got %d", len(instrs))
			}
			instr := instrs[0]
			if instr.Operand != tt.wantField {
				t.Errorf("field = %q, want %q", instr.Operand, tt.wantField)
			}
			if instr.Operator != tt.wantOp {
				t.Errorf("operator = %q, want %q", instr.Operator, tt.wantOp)
			}
			if instr.Value != tt.wantVal {
				t.Errorf("value = %q, want %q", instr.Value, tt.wantVal)
			}
		})
	}
}

func TestParseUnclosedParallel(t *testing.T) {
	_, err := Lex("p:a,b")
	if err != nil {
		t.Fatalf("Lex error: %v", err)
	}
	tokens, _ := Lex("p:a,b")
	_, err = Parse(tokens)
	if err == nil {
		t.Error("expected error for unclosed parallel block")
	}
}

func TestParseNestedParallel(t *testing.T) {
	tokens, _ := Lex("p:a,b p:c,d c")
	_, err := Parse(tokens)
	if err == nil {
		t.Error("expected error for nested parallel blocks")
	}
}

func TestParseDropAfterDrop(t *testing.T) {
	tokens, _ := Lex("d d")
	instrs, err := Parse(tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(instrs) != 1 {
		t.Errorf("expected 1 instruction (duplicate d removed), got %d", len(instrs))
	}
}
