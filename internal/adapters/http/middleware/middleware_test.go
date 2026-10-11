package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTenantExtractorDevelopmentUsesTenantHeader(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	req := httptest.NewRequest(http.MethodGet, "/v1/rules/active", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rec := httptest.NewRecorder()

	TenantExtractor(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := GetTenantID(r.Context()); got != "tenant-a" {
			t.Fatalf("GetTenantID() = %q, want tenant-a", got)
		}
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestTenantExtractorProductionRejectsCallerTenantHeader(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "proxy-secret")
	req := httptest.NewRequest(http.MethodGet, "/v1/rules/active", nil)
	req.Header.Set("X-Tenant-ID", "victim")
	rec := httptest.NewRecorder()

	TenantExtractor(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not be called without trusted proxy credentials")
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestTenantExtractorProductionUsesTrustedTenantHeader(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "proxy-secret")
	req := httptest.NewRequest(http.MethodGet, "/v1/rules/active", nil)
	req.Header.Set(trustedProxyTokenHeader, "proxy-secret")
	req.Header.Set(trustedTenantHeader, "tenant-a")
	req.Header.Set("X-Tenant-ID", "attacker-selected-tenant")
	rec := httptest.NewRecorder()

	TenantExtractor(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := GetTenantID(r.Context()); got != "tenant-a" {
			t.Fatalf("GetTenantID() = %q, want tenant-a", got)
		}
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestTenantExtractorProductionFailsClosedWhenProxyNotConfigured(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "")
	req := httptest.NewRequest(http.MethodGet, "/v1/rules/active", nil)
	rec := httptest.NewRecorder()

	TenantExtractor(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not be called when production tenant auth is unconfigured")
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}
