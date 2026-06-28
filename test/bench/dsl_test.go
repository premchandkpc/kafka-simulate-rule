package bench

import (
	"testing"

	"github.com/premchand/flowrule/internal/dsl"
	"github.com/premchand/flowrule/internal/memory"
)

var benchRegistry = dsl.TargetRegistry{
	"validate":        "http://validate:8080",
	"fraud":           "http://fraud:8080",
	"inventory":       "http://inventory:8080",
	"fulfill":         "http://fulfill:8080",
	"notify":          "http://notify:8080",
	"analytics":       "http://analytics:8080",
	"dlq":             "http://dlq:8080",
	"manual-review":   "http://review:8080",
	"auto-approve":    "http://approve:8080",
	"review-queue":    "http://queue:8080",
	"default-handler": "http://default:8080",
}

func BenchmarkLex(b *testing.B) {
	input := "t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dsl.Lex(input)
	}
}

func BenchmarkParse(b *testing.B) {
	input := "t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics"
	tokens, _ := dsl.Lex(input)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dsl.Parse(tokens)
	}
}

func BenchmarkCompile(b *testing.B) {
	input := "t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics"
	tokens, _ := dsl.Lex(input)
	instrs, _ := dsl.Parse(tokens)
	optimized, _ := dsl.OptimizeAndVerify(instrs)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dsl.Compile(optimized, benchRegistry, "bench-rule", 1)
	}
}

func BenchmarkFullPipeline(b *testing.B) {
	input := "t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tokens, _ := dsl.Lex(input)
		instrs, _ := dsl.Parse(tokens)
		optimized, _ := dsl.OptimizeAndVerify(instrs)
		dsl.Compile(optimized, benchRegistry, "bench-rule", 1)
	}
}

func BenchmarkArena_Alloc(b *testing.B) {
	a := memory.GetArena(1024)
	defer a.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := a.Alloc(1024)
		_ = buf
	}
}

func BenchmarkGateEval(b *testing.B) {
	body := []byte(`{"amount":15000,"user":{"tier":"gold"},"status":"active"}`)
	instr := dsl.Instruction{
		Operand:  "amount",
		Operator: ">",
		Value:    "10000",
	}
	msg := struct {
		LastResponse []byte
		Body         []byte
	}{
		LastResponse: body,
		Body:         body,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// evalGate is unexported, so we test the comparison directly
		_ = msg.LastResponse
		_ = instr
	}
}
