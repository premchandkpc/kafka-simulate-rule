package vm

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/premchand/flowrule/internal/bytecode"
	"github.com/premchand/flowrule/internal/dsl"
	"github.com/rs/zerolog"
)

func TestIntegration_CompileAndExecute(t *testing.T) {
	tests := []struct {
		name string
		dsl  string
		body string
		want int
		fail bool
	}{
		{
			name: "simple next",
			dsl:  `n:svc`,
			body: `{"ok":true}`,
			want: 1,
		},
		{
			name: "gate pass then next",
			dsl:  `g:user==alice n:svc`,
			body: `{"user":"alice"}`,
			want: 1,
		},
		{
			name: "gate skip then next",
			dsl:  `g:user==bob n:svc-a | n:svc-b`,
			body: `{"user":"alice"}`,
			want: 1,
		},
		{
			name: "parallel with collect",
			dsl:  `p:svc-a,svc-b c`,
			body: `{"ok":true}`,
			want: 1, // collect increments hop count
		},
		{
			name: "emit",
			dsl:  `e:svc`,
			body: `{"ok":true}`,
			want: 0,
		},
		{
			name: "next with retry",
			dsl:  `n:svc r3`,
			body: `{"ok":true}`,
			want: 1,
		},
		{
			name: "next with fallback",
			dsl:  `n:primary f:fallback`,
			body: `{"ok":true}`,
			want: 1,
		},
	}

	registry := dsl.TargetRegistry{
		"svc":      "http://svc:8080",
		"svc-a":    "http://svc-a:8080",
		"svc-b":    "http://svc-b:8080",
		"primary":  "http://primary:8080",
		"fallback": "http://fallback:8080",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens, err := dsl.Lex(tt.dsl)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := dsl.Parse(tokens)
			if err != nil {
				t.Fatal(err)
			}
			optimized, err := dsl.Optimize(raw)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := dsl.Compile(optimized, registry, "test-rule", 1)
			if err != nil {
				t.Fatal(err)
			}

			mod, err := bytecode.CompileFromDSL(plan)
			if err != nil {
				t.Fatal(err)
			}

			caller := fakeCaller(`{"result":"ok"}`)
			breaker := fakeBreaker(true)
			credit := fakeCredit(100)
			emitter := &testEmitter{
				fn: func(ctx context.Context, target string, body []byte) error {
					return nil
				},
			}

			v := New(caller, emitter, breaker, credit, zerolog.Nop())
			msg := testMsg(tt.body)
			err = v.Execute(context.Background(), msg, mod)
			if tt.fail {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if msg.HopCount != tt.want {
				t.Fatalf("hop count = %d, want %d", msg.HopCount, tt.want)
			}
		})
	}
}

func TestIntegration_EncodeDecodeRoundtrip(t *testing.T) {
	src := `g:user==alice n:svc`
	registry := dsl.TargetRegistry{"svc": "http://svc:8080"}

	tokens, err := dsl.Lex(src)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := dsl.Parse(tokens)
	if err != nil {
		t.Fatal(err)
	}
	opt, err := dsl.Optimize(raw)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := dsl.Compile(opt, registry, "roundtrip", 1)
	if err != nil {
		t.Fatal(err)
	}

	mod, err := bytecode.CompileFromDSL(plan)
	if err != nil {
		t.Fatal(err)
	}

	data, err := mod.Encode()
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := bytecode.Decode(data)
	if err != nil {
		t.Fatal(err)
	}

	if len(decoded.Instrs) != len(mod.Instrs) {
		t.Fatalf("instruction count: %d vs %d", len(decoded.Instrs), len(mod.Instrs))
	}
}

func TestIntegration_FileWriteRead(t *testing.T) {
	src := `g:user==alice n:svc r2 | e:events`
	registry := dsl.TargetRegistry{
		"svc":    "http://svc:8080",
		"events": "http://events:8080",
	}

	tokens, err := dsl.Lex(src)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := dsl.Parse(tokens)
	if err != nil {
		t.Fatal(err)
	}
	optimized, err := dsl.Optimize(raw)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := dsl.Compile(optimized, registry, "file-test", 1)
	if err != nil {
		t.Fatal(err)
	}

	mod, err := bytecode.CompileFromDSL(plan)
	if err != nil {
		t.Fatal(err)
	}

	data, err := mod.Encode()
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "test.flow")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	readback, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := bytecode.Decode(readback)
	if err != nil {
		t.Fatal(err)
	}

	if len(decoded.Instrs) != len(mod.Instrs) {
		t.Fatalf("instruction count mismatch")
	}
}

func TestIntegration_ParallelRoundtrip(t *testing.T) {
	src := `p:svc-a,svc-b c`
	registry := dsl.TargetRegistry{
		"svc-a": "http://svc-a:8080",
		"svc-b": "http://svc-b:8080",
	}

	tokens, err := dsl.Lex(src)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := dsl.Parse(tokens)
	if err != nil {
		t.Fatal(err)
	}
	optimized, err := dsl.Optimize(raw)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := dsl.Compile(optimized, registry, "parallel-test", 1)
	if err != nil {
		t.Fatal(err)
	}

	mod, err := bytecode.CompileFromDSL(plan)
	if err != nil {
		t.Fatal(err)
	}

	data, err := mod.Encode()
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := bytecode.Decode(data)
	if err != nil {
		t.Fatal(err)
	}

	foundParallel := false
	for _, instr := range decoded.Instrs {
		if instr.Opcode == bytecode.OpParallel {
			foundParallel = true
			break
		}
	}
	if !foundParallel {
		t.Fatal("expected Parallel instruction in decoded module")
	}
}
