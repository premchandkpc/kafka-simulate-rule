# Banking Example

This example demonstrates financial transaction processing with compliance checks and settlement.

## Architecture

```
TransactionInitiated
      │
      ▼
Rule: compliance check required? (amount >= $10,000 OR international)
      │
      ├── yes → ComplianceCheck
      │
      └── no  → TransactionSettled

ComplianceCheck
      │
      ▼
ComplianceResult
      │
      ├── passed → TransactionSettled
      │
      └── failed → TransactionBlocked
```

## Contracts

| Contract | Description |
|----------|-------------|
| `transaction.initiated` | New transaction initiated |
| `compliance.check` | Trigger compliance check |
| `compliance.result` | Compliance check result |
| `transaction.settled` | Transaction settled successfully |
| `transaction.blocked` | Transaction blocked due to compliance |

## Rules

| Rule Set | Description |
|----------|-------------|
| `transaction.initiated` | Determines if compliance check needed based on amount/geography |
| `compliance.result` | Routes to settlement or blocking based on compliance result |

## Workflow

The `transaction-processing` workflow defines the state machine:
- `initiated` → `compliance_pending` (on `compliance.check`)
- `initiated` → `settled` (on `transaction.settled` for low-risk)
- `compliance_pending` → `compliance_completed` (on `compliance.result`)
- `compliance_completed` → `settled` (on `transaction.settled`)
- `compliance_completed` → `blocked` (on `transaction.blocked`)

## Running the Example

### Prerequisites
- NATS JetStream server running on `localhost:4222`
- FlowRule API running on `localhost:8080`
- FlowRule Worker running

### 1. Register Contracts
```bash
for f in contracts/*.yaml; do
  curl -X POST http://localhost:8080/v1/contracts \
    -H "Content-Type: application/yaml" \
    -d @$f
done
```

### 2. Deploy Rules
```bash
for f in rules/*.yaml; do
  ruleSet=$(basename $f .yaml)
  curl -X POST http://localhost:8080/v1/rules/$ruleSet/revisions \
    -H "Content-Type: application/yaml" \
    -d @$f
done
```

### 3. Deploy Workflow
```bash
curl -X POST http://localhost:8080/v1/workflow-definitions \
  -H "Content-Type: application/yaml" \
  -d @workflows/transaction-processing.yaml
```

### 4. Run Producer
```bash
cd backend/producer
go run main.go
```

### 5. Run Consumer
```bash
cd backend/consumer
go run main.go
```

## Code Generation

Generate Go types from contracts:
```bash
flowrule-codegen -contract transaction.initiated -version 1.0 -target go -output generated/go/transaction_initiated.go
```

## Key Benefits

1. **Regulatory compliance** - Built-in audit trail for all transactions
2. **Risk-based routing** - Automatic compliance checks for high-risk transactions
3. **Type-safe contracts** - Generated types prevent schema drift
4. **Exactly-once processing** - Inbox pattern prevents duplicate settlements
5. **Observable** - Full lineage from transaction → compliance → settlement