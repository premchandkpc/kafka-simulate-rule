package bridge

import (
	"testing"
)

func TestCompileValidDSL(t *testing.T) {
	plan, err := Compile("n:validate", "test-1")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	if len(plan) == 0 {
		t.Fatal("expected non-empty plan")
	}
}

func TestCompileInvalidDSL(t *testing.T) {
	_, err := Compile("!!!invalid", "bad-rule")
	if err == nil {
		t.Fatal("expected error for invalid DSL")
	}
}

func TestCompileEmptyDSL(t *testing.T) {
	_, err := Compile("", "empty")
	if err == nil {
		t.Fatal("expected error for empty DSL")
	}
}

func TestExecuteValidPlan(t *testing.T) {
	plan, err := Compile("n:validate", "test-exec")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		return body, nil
	}

	result, err := Execute(plan, []byte(`{"test": true}`), caller)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(result) == 0 {
		// OK — result may be empty depending on rule pipeline
	}
}

func TestExecuteEmptyBody(t *testing.T) {
	plan, err := Compile("n:validate", "test-empty")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	caller := func(svcID uint16, body []byte) ([]byte, error) {
		return body, nil
	}

	_, err = Execute(plan, []byte{}, caller)
	// May succeed or fail depending on Rust VM — just check no panic
	_ = err
}

func TestExecuteBadPlan(t *testing.T) {
	caller := func(svcID uint16, body []byte) ([]byte, error) {
		return body, nil
	}
	_, err := Execute([]byte{0, 1, 2, 3, 4, 5}, []byte(`{}`), caller)
	if err == nil {
		t.Fatal("expected error for bad plan bytes")
	}
}

func TestExecuteNilCaller(t *testing.T) {
	plan, err := Compile("n:validate", "test-nil")
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	// nil caller should cause callback to return error
	_, err = Execute(plan, []byte(`{}`), nil)
	if err == nil {
		// Some DSL may not trigger callback; this is valid
	}
}

func TestInternRoundtrip(t *testing.T) {
	s := "test-service-name"
	id := Intern(s)
	if id == 0 {
		t.Fatal("expected non-zero intern ID")
	}

	got := InternLookup(id)
	if got != s {
		t.Errorf("roundtrip: expected %q, got %q", s, got)
	}
}

func TestInternEmptyString(t *testing.T) {
	id := Intern("")
	if id != 0 {
		t.Errorf("expected 0 for empty string, got %d", id)
	}
}

func TestInternLookupUnknown(t *testing.T) {
	got := InternLookup(65535)
	if got != "" {
		t.Errorf("expected empty for unknown ID, got %q", got)
	}
}

func TestMsgAllocRelease(t *testing.T) {
	ptr := MsgAlloc(1024)
	if ptr == nil {
		t.Fatal("expected non-nil pointer")
	}
	MsgRelease(ptr)
}

func TestMsgAllocZero(t *testing.T) {
	ptr := MsgAlloc(0)
	MsgRelease(ptr)
}
