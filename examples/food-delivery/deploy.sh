#!/bin/bash
# Deploy food-delivery example contracts, rules, and workflow

set -e

API_URL="${FLOWRULE_API:-http://localhost:8080}"
FLOWRULE_ROOT="${FLOWRULE_ROOT:-../../..}"

echo "Deploying food-delivery example to $API_URL"

echo "Generating Go types from contracts..."
mkdir -p generated/go
for f in contracts/*.yaml; do
  echo "  Generating $f"
  "$FLOWRULE_ROOT/bin/codegen-yaml" -contract "$f" -output generated -target go
done

echo "Registering contracts..."
for f in contracts/*.yaml; do
  echo "  Registering $f"
  curl -s -X POST "$API_URL/v1/contracts" \
    -H "Content-Type: application/yaml" \
    -d @"$f" | jq .
done

echo "Deploying rules..."
for f in rules/*.yaml; do
  ruleSet=$(basename "$f" .yaml)
  echo "  Deploying $ruleSet"
  curl -s -X POST "$API_URL/v1/rules/$ruleSet/revisions" \
    -H "Content-Type: application/yaml" \
    -d @"$f" | jq .
done

echo "Deploying workflow..."
curl -s -X POST "$API_URL/v1/workflow-definitions" \
  -H "Content-Type: application/yaml" \
  -d @workflows/food-delivery.yaml | jq .

echo "Food delivery deployment complete!"