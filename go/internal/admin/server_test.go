package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/premchandkpc/kafka-simulate-rule/go/internal/engine"
)

func TestHealth(t *testing.T) {
	eng := engine.New()
	srv := New(eng)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "ok" {
		t.Errorf("expected 'ok', got %s", resp["status"])
	}
}

func TestDeployAndListRules(t *testing.T) {
	eng := engine.New()
	srv := New(eng)

	body := `{"id":"test-1","dsl":"n:validate"}`
	req := httptest.NewRequest("POST", "/rules", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	req = httptest.NewRequest("GET", "/rules", nil)
	w = httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var rules []map[string]string
	json.NewDecoder(w.Body).Decode(&rules)
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	if rules[0]["id"] != "test-1" {
		t.Errorf("expected test-1, got %s", rules[0]["id"])
	}
}

func TestRemoveRule(t *testing.T) {
	eng := engine.New()
	srv := New(eng)

	body := `{"id":"test-1","dsl":"n:validate"}`
	req := httptest.NewRequest("POST", "/rules", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	req = httptest.NewRequest("DELETE", "/rules/test-1", nil)
	w = httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", w.Code)
	}
}

func TestDeployInvalidDSL(t *testing.T) {
	eng := engine.New()
	srv := New(eng)

	body := `{"id":"bad","dsl":"!!!invalid"}`
	req := httptest.NewRequest("POST", "/rules", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}
