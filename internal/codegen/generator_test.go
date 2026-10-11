package codegen

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/flowrule/flowrule/internal/domain"
)

func TestGenerateGoIncludesTypedClient(t *testing.T) {
	outputDir := t.TempDir()
	generator := NewGenerator(&domain.ContractSchema{
		Name:    "order.created",
		Version: "1.0",
		Fields: map[string]domain.ContractField{
			"order_id": {Type: "string", Required: true},
		},
	})

	files, err := generator.GenerateGo(outputDir)
	if err != nil {
		t.Fatalf("GenerateGo() error = %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("GenerateGo() returned %d files, want contract and typed client", len(files))
	}

	for _, path := range files {
		if _, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors); err != nil {
			t.Errorf("generated Go file %s does not parse: %v", path, err)
		}
	}

	contractPath := filepath.Join(outputDir, TargetGo, "created.go")
	contractContent, err := os.ReadFile(contractPath)
	if err != nil {
		t.Fatalf("read generated contract: %v", err)
	}
	if !strings.Contains(string(contractContent), "OrderId string `json:\"order_id\"`") {
		t.Errorf("generated Go contract does not expose the order_id field as OrderId")
	}

	clientPath := filepath.Join(outputDir, TargetGo, "created_client.go")
	content, err := os.ReadFile(clientPath)
	if err != nil {
		t.Fatalf("read generated client: %v", err)
	}
	for _, expected := range []string{
		`const DefaultRuleSet = "order.created"`,
		"func (c *Client) SendToRuleSet(",
		"func (c *Client) SubscribeRuleSet(",
		"opts ...flowrule.SendOption",
		"c.client.Subscribe(ctx, ruleSet",
	} {
		if !strings.Contains(string(content), expected) {
			t.Errorf("generated client does not contain %q", expected)
		}
	}
}

func TestToGoIdentifier(t *testing.T) {
	tests := map[string]string{
		"order_id":  "OrderId",
		"order-id":  "OrderId",
		"1st_order": "Field1stOrder",
		"!!!":       "Field",
	}
	for input, want := range tests {
		t.Run(input, func(t *testing.T) {
			if got := toGoIdentifier(input); got != want {
				t.Errorf("toGoIdentifier(%q) = %q, want %q", input, got, want)
			}
		})
	}
}
