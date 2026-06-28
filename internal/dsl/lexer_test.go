package dsl

import (
	"testing"
)

func TestLex(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []TokenType
		wantErr bool
	}{
		{
			name:  "timeout",
			input: "t500",
			want:  []TokenType{TokenTimeout},
		},
		{
			name:  "next",
			input: "n:validate",
			want:  []TokenType{TokenNext},
		},
		{
			name:  "parallel",
			input: "p:fraud,inventory",
			want:  []TokenType{TokenParallel},
		},
		{
			name:  "collect",
			input: "c",
			want:  []TokenType{TokenCollect},
		},
		{
			name:  "retry",
			input: "r3",
			want:  []TokenType{TokenRetry},
		},
		{
			name:  "fallback",
			input: "f:dlq",
			want:  []TokenType{TokenFallback},
		},
		{
			name:  "gate",
			input: "g:amount>10000",
			want:  []TokenType{TokenGate},
		},
		{
			name:  "pipe",
			input: "|",
			want:  []TokenType{TokenPipe},
		},
		{
			name:  "split",
			input: "s:region",
			want:  []TokenType{TokenSplit},
		},
		{
			name:  "map",
			input: "m:.result",
			want:  []TokenType{TokenMap},
		},
		{
			name:  "emit",
			input: "e:email,sms",
			want:  []TokenType{TokenEmit},
		},
		{
			name:  "drop",
			input: "d",
			want:  []TokenType{TokenDrop},
		},
		{
			name:  "buffer",
			input: "b10",
			want:  []TokenType{TokenBuffer},
		},
		{
			name:  "key",
			input: "k:user_id",
			want:  []TokenType{TokenKey},
		},
		{
			name:  "full pipeline",
			input: "t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics",
			want: []TokenType{
				TokenTimeout, TokenNext, TokenTimeout,
				TokenParallel, TokenCollect, TokenFallback,
				TokenNext, TokenEmit,
			},
		},
		{
			name:  "gate with pipe",
			input: "g:amount>10000 n:manual-review | t500 n:auto-approve f:review-queue",
			want: []TokenType{
				TokenGate, TokenNext, TokenPipe,
				TokenTimeout, TokenNext, TokenFallback,
			},
		},
		{
			name:    "invalid token",
			input:   "zzz",
			wantErr: true,
		},
		{
			name:    "n without target",
			input:   "n:",
			wantErr: true,
		},
		{
			name:    "t without digits",
			input:   "t",
			wantErr: true,
		},
		{
			name:    "r without digits",
			input:   "r",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Lex(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Lex(%q) expected error, got %v", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Errorf("Lex(%q) unexpected error: %v", tt.input, err)
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("Lex(%q) len=%d, want %d", tt.input, len(got), len(tt.want))
				return
			}
			for i, tok := range got {
				if tok.Type != tt.want[i] {
					t.Errorf("Lex(%q)[%d] = %s, want %s", tt.input, i, tok.Type, tt.want[i])
				}
			}
		})
	}
}
