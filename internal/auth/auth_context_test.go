package auth

import (
	"context"
	"testing"
)

func TestContextWithClaimsPreservesTenantAndScopes(t *testing.T) {
	claims := &Claims{TenantID: "tenant-a", Scopes: []string{"contracts:admin"}}
	ctx := ContextWithClaims(context.Background(), claims)

	if tenant, ok := TenantFromContext(ctx); !ok || tenant != "tenant-a" {
		t.Fatalf("TenantFromContext() = (%q, %v), want (tenant-a, true)", tenant, ok)
	}
	if !HasScope(ctx, "contracts:admin") {
		t.Fatal("HasScope() = false, want contracts:admin")
	}
	if HasScope(ctx, "rules:write") {
		t.Fatal("HasScope() = true for an ungranted scope")
	}
}

func TestHasScopeSupportsWildcard(t *testing.T) {
	ctx := ContextWithClaims(context.Background(), &Claims{TenantID: "tenant-a", Scopes: []string{"*"}})
	if !HasScope(ctx, "contracts:admin") {
		t.Fatal("wildcard scope should authorize the requested scope")
	}
}

func TestContextWithNilClaimsDoesNotInventIdentity(t *testing.T) {
	ctx := ContextWithClaims(context.Background(), nil)
	if _, ok := TenantFromContext(ctx); ok {
		t.Fatal("nil claims must not establish tenant identity")
	}
	if HasScope(ctx, "admin") {
		t.Fatal("nil claims must not grant scopes")
	}
}
