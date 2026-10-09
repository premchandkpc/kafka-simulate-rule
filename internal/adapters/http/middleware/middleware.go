package middleware

import (
	"context"
	"net/http"
)

// TenantContextKey is the context key for tenant information
type TenantContextKey string

const (
	TenantIDKey TenantContextKey = "tenant_id"
)

// TenantExtractor extracts tenant ID from request
func TenantExtractor(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantID := extractTenantID(r)
		if tenantID != "" {
			ctx := context.WithValue(r.Context(), TenantIDKey, tenantID)
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(w, r)
	})
}

// extractTenantID extracts tenant ID from various sources
func extractTenantID(r *http.Request) string {
	// Check header first (for multi-tenant testing)
	if tenant := r.Header.Get("X-Tenant-ID"); tenant != "" {
		return tenant
	}
	// Check query param
	if tenant := r.URL.Query().Get("tenant_id"); tenant != "" {
		return tenant
	}
	// Default for single-tenant deployments
	return "default"
}

// GetTenantID retrieves tenant ID from context
func GetTenantID(ctx context.Context) string {
	if tenant, ok := ctx.Value(TenantIDKey).(string); ok {
		return tenant
	}
	return "default"
}

// Recovery middleware recovers from panics
func Recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				// In production, log the panic with stack trace
				http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}