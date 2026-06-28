package vm

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/premchand/flowrule/internal/bytecode"
	"github.com/rs/zerolog"
)

func TestVM_Next_Success(t *testing.T) {
	m, log := testMod()
	vm := New(fakeCaller("ok"), nil, fakeBreaker(true), fakeCredit(5), log)
	msg := testMsg(`{"user":"alice"}`)
	err := vm.Execute(context.Background(), msg, m)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Failed() {
		t.Fatal("expected success")
	}
	if msg.HopCount != 1 {
		t.Fatalf("hop count = %d, want 1", msg.HopCount)
	}
}

func TestVM_Next_CircuitOpen(t *testing.T) {
	m, log := testMod()
	vm := New(fakeCaller("ok"), nil, fakeBreaker(false), fakeCredit(5), log)
	msg := testMsg(`{"user":"alice"}`)
	err := vm.Execute(context.Background(), msg, m)
	if err == nil {
		t.Fatal("expected error due to open circuit")
	}
	if !msg.Failed() {
		t.Fatal("expected failed")
	}
}

func TestVM_Gate_Pass(t *testing.T) {
	m, log := emptyMod()
	// Gate checks "user" == "alice" on the original Body
	m.Instrs = append(m.Instrs, bytecode.Instruction{
		Opcode: bytecode.OpGate,
		Arg1:   addFieldCP(m, "user"),
		Arg2:   addOperatorCP(m, "=="),
		Arg3:   addValueCP(m, "alice"),
	})
	addNext(m)

	vm := New(fakeCaller(`{"ok":true}`), nil, fakeBreaker(true), fakeCredit(5), log)
	msg := testMsg(`{"user":"alice"}`)
	err := vm.Execute(context.Background(), msg, m)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Failed() {
		t.Fatal("expected success")
	}
	if msg.HopCount != 1 {
		t.Fatalf("hop count = %d, want 1", msg.HopCount)
	}
}

func TestVM_Gate_Skip(t *testing.T) {
	m, log := emptyMod()
	// Gate: user == "bob" — body has "alice", so gate skips
	m.Instrs = append(m.Instrs, bytecode.Instruction{
		Opcode: bytecode.OpGate,
		Arg1:   addFieldCP(m, "user"),
		Arg2:   addOperatorCP(m, "=="),
		Arg3:   addValueCP(m, "bob"),
	})
	addNext(m) // skipped by gate
	m.Instrs = append(m.Instrs, bytecode.Instruction{Opcode: bytecode.OpPipe})
	addNext(m) // executed after pipe

	vm := New(fakeCaller(`{"ok":true}`), nil, fakeBreaker(true), fakeCredit(5), log)
	msg := testMsg(`{"user":"alice"}`)
	err := vm.Execute(context.Background(), msg, m)
	if err != nil {
		t.Fatal(err)
	}
	if msg.HopCount != 1 {
		t.Fatalf("hop count = %d, want 1 (skipped block)", msg.HopCount)
	}
}

func TestVM_Gate_MultiSegment(t *testing.T) {
	t.Skip("needs proper multi-segment constant pool setup")
}

func TestVM_Parallel(t *testing.T) {
	m, log := testModWithParallel()
	vm := New(fakeCaller(`{"ok":true}`), nil, fakeBreaker(true), fakeCredit(5), log)
	msg := testMsg(`{"data":"test"}`)
	err := vm.Execute(context.Background(), msg, m)
	if err != nil {
		t.Fatal(err)
	}
}

func TestVM_Drop(t *testing.T) {
	m, log := emptyMod()
	m.Instrs = append(m.Instrs, bytecode.Instruction{Opcode: bytecode.OpDrop})
	vm := New(fakeCaller("ok"), nil, fakeBreaker(true), fakeCredit(5), log)
	msg := testMsg(`{"user":"alice"}`)
	err := vm.Execute(context.Background(), msg, m)
	if err != nil {
		t.Fatal(err)
	}
}

func TestVM_Emit(t *testing.T) {
	m, log := emptyMod()
	addModURLs(m)
	done := make(chan struct{})
	emitter := &testEmitter{
		fn: func(ctx context.Context, target string, body []byte) error {
			close(done)
			return nil
		},
	}

	m.TargetLists = append(m.TargetLists, bytecode.TargetList{
		Indices: []uint32{0},
	})
	m.Instrs = append(m.Instrs, bytecode.Instruction{
		Opcode: bytecode.OpEmit,
		Arg1:   0,
	})

	vm := New(fakeCaller("ok"), emitter, fakeBreaker(true), fakeCredit(5), log)
	msg := testMsg(`{"user":"alice"}`)
	err := vm.Execute(context.Background(), msg, m)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected emit to be called")
	}
}

func TestVM_Map_OnBody(t *testing.T) {
	m, log := emptyMod()
	// Map expression extracts "user" from the Body (no Next, so LastResponse is Body)
	userIdx := addStringCP(m, "user")
	body := make([]byte, 8)
	body[0] = 1
	body[1] = 0
	body[4] = byte(userIdx)
	body[5] = byte(userIdx >> 8)
	body[6] = byte(userIdx >> 16)
	body[7] = byte(userIdx >> 24)
	m.MapExprs = append(m.MapExprs, bytecode.MapExprEntry{
		Type: bytecode.MapExprFieldPath,
		Body: body,
	})
	m.Instrs = append(m.Instrs, bytecode.Instruction{
		Opcode: bytecode.OpMap,
		Arg1:   0,
	})

	vm := New(nil, nil, fakeBreaker(true), fakeCredit(5), log)
	msg := testMsg(`{"user":"alice"}`)
	err := vm.Execute(context.Background(), msg, m)
	if err != nil {
		t.Fatal(err)
	}
	// After Map, LastResponse should be the extracted field
	var result string
	if err := json.Unmarshal(msg.LastResponse, &result); err != nil {
		t.Fatal(err)
	}
	if result != "alice" {
		t.Fatalf("expected 'alice', got %q", result)
	}
}

func TestVM_Jump(t *testing.T) {
	m, log := emptyMod()
	addModURLs(m)
	// Build instructions manually: jump past first next
	m.Instrs = []bytecode.Instruction{
		{Opcode: bytecode.OpJump, Arg1: 2}, // jump to instr[2]
		{Opcode: bytecode.OpNext, Arg1: 0}, // skipped
		{Opcode: bytecode.OpNext, Arg1: 0}, // executed
	}

	vm := New(fakeCaller(`{"ok":true}`), nil, fakeBreaker(true), fakeCredit(5), log)
	msg := testMsg(`{"user":"alice"}`)
	err := vm.Execute(context.Background(), msg, m)
	if err != nil {
		t.Fatal(err)
	}
	if msg.HopCount != 1 {
		t.Fatalf("hop count = %d, want 1", msg.HopCount)
	}
}

func TestVM_JumpIf_Failed(t *testing.T) {
	m, log := emptyMod()
	addModURLs(m)
	// First Next fails (caller returns error), JumpIf jumps past skipped block
	m.Instrs = []bytecode.Instruction{
		{Opcode: bytecode.OpNext, Arg1: 0}, // will fail
		{Opcode: bytecode.OpJumpIf, Arg1: 3}, // jump to instr[3] on failure
		{Opcode: bytecode.OpNext, Arg1: 0}, // skipped
		{Opcode: bytecode.OpNext, Arg1: 0}, // executed
	}

	vm := New(fakeCallerWithFailure(1), nil, fakeBreaker(true), fakeCredit(5), log)
	msg := testMsg(`{"user":"alice"}`)
	err := vm.Execute(context.Background(), msg, m)
	if err != nil {
		t.Fatal(err)
	}
	if msg.HopCount != 1 {
		t.Fatalf("hop count = %d, want 1", msg.HopCount)
	}
}

func TestVM_JumpIfN_Success(t *testing.T) {
	m, log := emptyMod()
	addModURLs(m)
	m.Instrs = []bytecode.Instruction{
		{Opcode: bytecode.OpNext, Arg1: 0}, // succeeds
		{Opcode: bytecode.OpJumpIfN, Arg1: 3}, // jump to instr[3] on success
		{Opcode: bytecode.OpNext, Arg1: 0}, // skipped
		{Opcode: bytecode.OpNext, Arg1: 0}, // executed
	}

	vm := New(fakeCaller("ok"), nil, fakeBreaker(true), fakeCredit(5), log)
	msg := testMsg(`{"user":"alice"}`)
	err := vm.Execute(context.Background(), msg, m)
	if err != nil {
		t.Fatal(err)
	}
	if msg.HopCount != 2 {
		t.Fatalf("hop count = %d, want 2", msg.HopCount)
	}
}

func TestVM_Next_WithRetry(t *testing.T) {
	m, log := emptyMod()
	addModURLs(m)
	// Next with retry flag
	m.Instrs = append(m.Instrs, bytecode.Instruction{
		Opcode: bytecode.OpNext,
		Flags:  bytecode.FlagHasRetry,
		Arg1:   0,  // url cp index
		Arg2:   2,  // retry count
	})

	callCount := 0
	caller := CallerFunc(func(ctx context.Context, target string, body []byte) ([]byte, error) {
		callCount++
		if callCount < 3 {
			return nil, errFail
		}
		return []byte("ok"), nil
	})

	vm := New(caller, nil, fakeBreaker(true), fakeCredit(10), log)
	msg := testMsg(`{"user":"alice"}`)
	err := vm.Execute(context.Background(), msg, m)
	if err != nil {
		t.Fatal(err)
	}
	if msg.HopCount != 1 {
		t.Fatalf("hop count = %d, want 1", msg.HopCount)
	}
	if callCount != 3 {
		t.Fatalf("call count = %d, want 3", callCount)
	}
}

func TestVM_Next_Fallback(t *testing.T) {
	m, log := emptyMod()
	addModURLs(m)
	addURL(m, "http://fallback")
	// First Next fails, fallback executes
	m.Instrs = []bytecode.Instruction{
		{Opcode: bytecode.OpNext, Arg1: 0},
		{Opcode: bytecode.OpFallback, Arg1: 1}, // fallback to url cp[1]
	}

	vm := New(fakeCallerWithFailure(1), nil, fakeBreaker(true), fakeCredit(5), log)
	msg := testMsg(`{"user":"alice"}`)
	err := vm.Execute(context.Background(), msg, m)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Failed() {
		t.Fatal("expected success after fallback")
	}
}

func TestVM_Key(t *testing.T) {
	m, log := emptyMod()
	addModURLs(m)
	addFieldCP(m, "user")
	m.Instrs = []bytecode.Instruction{
		{Opcode: bytecode.OpNext, Arg1: 0},
		{Opcode: bytecode.OpKey, Arg1: 1},
	}

	vm := New(fakeCaller(`{"ok":true,"user":"alice"}`), nil, fakeBreaker(true), fakeCredit(5), log)
	msg := testMsg(`{}`)
	err := vm.Execute(context.Background(), msg, m)
	if err != nil {
		t.Fatal(err)
	}
	if msg.PartitionKey != "alice" {
		t.Fatalf("partition key = %q, want 'alice'", msg.PartitionKey)
	}
}

func TestVM_PipeSkip(t *testing.T) {
	m, log := emptyMod()
	addModURLs(m)
	// Pipe should skip everything until the next Pipe or end
	m.Instrs = []bytecode.Instruction{
		{Opcode: bytecode.OpPipe},
		{Opcode: bytecode.OpNext, Arg1: 0}, // skipped
		{Opcode: bytecode.OpNext, Arg1: 0}, // skipped
	}

	vm := New(fakeCaller("ok"), nil, fakeBreaker(true), fakeCredit(5), log)
	msg := testMsg(`{"user":"alice"}`)
	err := vm.Execute(context.Background(), msg, m)
	if err != nil {
		t.Fatal(err)
	}
	if msg.HopCount != 0 {
		t.Fatalf("hop count = %d, want 0", msg.HopCount)
	}
}

// --- helpers ---

var errFail = &errType{}

type errType struct{}

func (e *errType) Error() string { return "fail" }

func emptyMod() (*bytecode.Module, zerolog.Logger) {
	return &bytecode.Module{
		VersionMajor: bytecode.VersionMajor,
		VersionMinor: bytecode.VersionMinor,
	}, zerolog.Nop()
}

func testMod() (*bytecode.Module, zerolog.Logger) {
	m, log := emptyMod()
	addModURLs(m)
	addNext(m)
	return m, log
}

func addModURLs(m *bytecode.Module) {
	m.ConstPool = append(m.ConstPool, bytecode.ConstEntry{
		Type:    bytecode.ConstURL,
		Payload: []byte("http://localhost:8080"),
	})
}

func addURL(m *bytecode.Module, url string) {
	m.ConstPool = append(m.ConstPool, bytecode.ConstEntry{
		Type:    bytecode.ConstURL,
		Payload: []byte(url),
	})
}

func addStringCP(m *bytecode.Module, s string) uint32 {
	idx := uint32(len(m.ConstPool))
	m.ConstPool = append(m.ConstPool, bytecode.ConstEntry{
		Type:    bytecode.ConstString,
		Payload: []byte(s),
	})
	return idx
}

func addFieldCP(m *bytecode.Module, s string) uint32 {
	idx := uint32(len(m.ConstPool))
	m.ConstPool = append(m.ConstPool, bytecode.ConstEntry{
		Type:    bytecode.ConstFieldPath,
		Payload: []byte(s),
	})
	return idx
}

func addOperatorCP(m *bytecode.Module, op string) uint32 {
	idx := uint32(len(m.ConstPool))
	m.ConstPool = append(m.ConstPool, bytecode.ConstEntry{
		Type:    bytecode.ConstOperator,
		Payload: []byte(op),
	})
	return idx
}

func addValueCP(m *bytecode.Module, v string) uint32 {
	idx := uint32(len(m.ConstPool))
	m.ConstPool = append(m.ConstPool, bytecode.ConstEntry{
		Type:    bytecode.ConstValue,
		Payload: []byte(v),
	})
	return idx
}

func addNext(m *bytecode.Module) {
	m.Instrs = append(m.Instrs, bytecode.Instruction{
		Opcode: bytecode.OpNext,
		Arg1:   0,
	})
}

func testModWithParallel() (*bytecode.Module, zerolog.Logger) {
	log := zerolog.Nop()
	m := &bytecode.Module{
		VersionMajor: bytecode.VersionMajor,
		VersionMinor: bytecode.VersionMinor,
	}
	m.ConstPool = append(m.ConstPool,
		bytecode.ConstEntry{Type: bytecode.ConstURL, Payload: []byte("http://svc-a:8080")},
		bytecode.ConstEntry{Type: bytecode.ConstURL, Payload: []byte("http://svc-b:8080")},
	)
	m.TargetLists = append(m.TargetLists, bytecode.TargetList{
		Indices: []uint32{0, 1},
	})
	m.Instrs = append(m.Instrs, bytecode.Instruction{
		Opcode: bytecode.OpParallel,
		Arg1:   0,
	})
	m.Instrs = append(m.Instrs, bytecode.Instruction{
		Opcode: bytecode.OpCollect,
	})
	return m, log
}

func testMsg(body string) *Message {
	return &Message{
		ID:       1,
		Body:     []byte(body),
		HopCount: 0,
	}
}

type testEmitter struct {
	fn func(ctx context.Context, target string, body []byte) error
}

func (e *testEmitter) Emit(ctx context.Context, target string, body []byte) error {
	return e.fn(ctx, target, body)
}

func fakeCaller(resp string) Caller {
	return CallerFunc(func(ctx context.Context, target string, body []byte) ([]byte, error) {
		return []byte(resp), nil
	})
}

func fakeCallerWithFailure(n int) Caller {
	i := 0
	return CallerFunc(func(ctx context.Context, target string, body []byte) ([]byte, error) {
		i++
		if i <= n {
			return nil, errFail
		}
		return []byte("ok"), nil
	})
}

type CallerFunc func(ctx context.Context, target string, body []byte) ([]byte, error)

func (f CallerFunc) Call(ctx context.Context, target string, body []byte) ([]byte, error) {
	return f(ctx, target, body)
}

func fakeBreaker(allow bool) Breaker {
	return &testBreaker{allow: allow}
}

type testBreaker struct {
	allow bool
}

func (b *testBreaker) Allow(target string) bool   { return b.allow }
func (b *testBreaker) RecordSuccess(target string) {}
func (b *testBreaker) RecordFailure(target string) {}

func fakeCredit(n int) Credit {
	return &testCredit{n: n}
}

type testCredit struct {
	n int
}

func (c *testCredit) CanSend(target string) bool {
	c.n--
	return c.n >= 0
}
func (c *testCredit) Debit(target string)  {}
func (c *testCredit) Credit(target string) {}
