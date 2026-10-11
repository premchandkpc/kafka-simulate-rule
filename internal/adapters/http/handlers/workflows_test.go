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

type workflowServiceStub struct {
	ports.WorkflowService
	createCalls int
}

func (s *workflowServiceStub) Create(context.Context, *domain.WorkflowInstance) error {
	s.createCalls++
	return nil
}

func TestCreateWorkflowRejectsTenantMismatch(t *testing.T) {
	service := &workflowServiceStub{}
	handler := NewWorkflowHandler(service, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows", strings.NewReader(
		`{"tenant_id":"tenant-b","workflow_type":"order","state":"pending"}`,
	))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", "tenant-a")
	response := httptest.NewRecorder()

	middleware.TenantExtractor(http.HandlerFunc(handler.CreateWorkflow)).ServeHTTP(response, req)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusForbidden, response.Body.String())
	}
	if service.createCalls != 0 {
		t.Fatalf("Create calls = %d, want 0", service.createCalls)
	}
}
