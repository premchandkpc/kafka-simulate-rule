package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flowrule/flowrule/internal/adapters/http/middleware"
	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

type batchServiceStub struct {
	ports.BatchService
	createCalls int
}

func (s *batchServiceStub) CreateBatch(context.Context, string, string, string, []string) (*domain.BatchRun, error) {
	s.createCalls++
	return nil, nil
}

func TestCreateBatchRejectsTenantMismatch(t *testing.T) {
	service := &batchServiceStub{}
	handler := NewBatchHandler(service)
	req := httptest.NewRequest(http.MethodPost, "/v1/batches", strings.NewReader(
		`{"tenant_id":"tenant-b","partition_key":"order-1","rule_set":"order.created","event_ids":["event-1"]}`,
	))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "tenant-a")
	response := httptest.NewRecorder()

	middleware.TenantExtractor(http.HandlerFunc(handler.CreateBatch)).ServeHTTP(response, req)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusForbidden, response.Body.String())
	}
	if service.createCalls != 0 {
		t.Fatalf("CreateBatch calls = %d, want 0", service.createCalls)
	}
}
