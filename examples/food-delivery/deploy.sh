#!/bin/bash
# Deploy food-delivery example contracts, rules, and workflow

set -e

API_URL="${FLOWRULE_API:-http://localhost:8080}"

echo "Deploying food-delivery example to $API_URL"

# Generated source files are checked in; deploy only registers the contract/rule/workflow definitions.

echo "Registering contracts..."
for f in contracts/*.yaml; do
  echo "  Registering $f"
  curl --fail-with-body -sS -X POST "$API_URL/v1/contracts" \
    -H "Content-Type: application/yaml" \
    -d @"$f" | jq .
done

echo "Deploying rules..."
for f in rules/*.yaml; do
  ruleSet=$(basename "$f" .yaml)
  echo "  Deploying $ruleSet"
  curl --fail-with-body -sS -X POST "$API_URL/v1/rules/$ruleSet/revisions" \
    -H "Content-Type: application/yaml" \
    -d @"$f" | jq .
done

echo "Deploying workflow..."
curl --fail-with-body -sS -X POST "$API_URL/v1/workflows/definitions" \
  -H "Content-Type: application/yaml" \
  -d @workflows/food-delivery.yaml | jq .

echo "Food delivery deployment complete!"