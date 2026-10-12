#!/bin/bash
# Deploy food-delivery example contracts, rules

set -e

API_URL="${FLOWRULE_API:-http://localhost:8080}"
TENANT_ID="${FLOWRULE_TENANT:-development-tenant}"

echo "Deploying food-delivery example to $API_URL"

# Generated code lives in ../../domain-builds (make generate-domain); deploy only registers contracts and rules.

echo "Registering contracts..."
for f in contracts/*.yaml; do
  echo "  Registering $f"
  curl --fail-with-body -sS -X POST "$API_URL/v1/contracts" \
    -H "X-Tenant-ID: $TENANT_ID" -H "Content-Type: application/yaml" \
    -d @"$f" | jq .
done

echo "Deploying rules..."
for f in rules/*.yaml; do
  ruleSet=$(basename "$f" .yaml)
  echo "  Deploying $ruleSet"
  curl --fail-with-body -sS -X POST "$API_URL/v1/rules/$ruleSet/revisions" \
    -H "X-Tenant-ID: $TENANT_ID" -H "Content-Type: application/yaml" \
    -d @"$f" | jq .
done

# Workflow definitions (workflows/*.yaml) are reference files: the tenant API does not expose workflow mutation.

echo "Food delivery deployment complete!"