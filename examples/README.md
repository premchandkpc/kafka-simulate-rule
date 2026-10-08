# FlowRule Examples

This directory contains example applications built on top of the FlowRule event-driven business runtime platform.

## Examples

### 🛒 E-commerce (`examples/ecommerce/`)
Complete order processing flow demonstrating:
- Order creation → Payment → Fraud Check → Shipment
- Rules for payment requirements, fraud detection, auto-confirmation
- Workflow with 7 states and conditional transitions
- 8 contract definitions with full schema

### 🍕 Food Delivery (`examples/food-delivery/`)
Food delivery order flow demonstrating:
- Order → Restaurant Accept → Food Prepared → Driver Assignment → Delivered
- Rules for auto-acceptance based on restaurant rating
- Driver assignment with geographic partitioning
- Workflow with 6 states

### 🏦 Banking (`examples/banking/`)
Financial transaction processing demonstrating:
- Transaction initiation → Compliance check → Settlement/Block
- Rules for high-value and international transaction compliance
- Risk-based routing with audit trail
- Workflow with 4 states

## Quick Start

### 1. Start Infrastructure
```bash
# Start NATS JetStream
docker run -d -p 4222:4222 -p 8222:8222 nats:latest -js

# Start PostgreSQL
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=flowrule postgres:15
```

### 2. Run FlowRule API
```bash
cd /path/to/flowrule
DATABASE_URL="postgres://postgres:postgres@localhost:5432/flowrule?sslmode=disable" \
MIGRATIONS_DIR="migrations" \
go run ./cmd/api
```

### 3. Run FlowRule Worker
```bash
MONGODB_URI="mongodb://localhost:27017" \
NATS_URL="nats://localhost:4222" \
go run ./cmd/worker
```

### 4. Deploy Example Contracts & Rules
```bash
# E-commerce
cd examples/ecommerce
./deploy.sh

# Food Delivery
cd examples/food-delivery
./deploy.sh

# Banking
cd examples/banking
./deploy.sh
```

### 5. Run Producer/Consumer
```bash
# E-commerce producer
cd examples/ecommerce/backend/producer
go run main.go

# E-commerce consumer
cd examples/ecommerce/backend/consumer
go run main.go
```

## Code Generation

All examples support code generation from contracts:

```bash
# Generate Go types
flowrule-codegen -contract order.created -version 1.0 -target go

# Generate Java classes
flowrule-codegen -contract order.created -version 1.0 -target java

# Generate Protobuf
flowrule-codegen -contract order.created -version 1.0 -target protobuf

# Generate JSON Schema
flowrule-codegen -contract order.created -version 1.0 -target jsonschema
```

## Architecture Pattern

These examples demonstrate the core FlowRule philosophy:

> **Define contracts + rules + workflows, let the runtime execute them.**

Instead of building:
- `order-service`
- `payment-service`
- `fraud-service`
- `shipment-service`
- `notification-service`

You define:
- **Contracts** (schemas for events/commands/effects)
- **Rules** (business logic as declarative predicates + actions)
- **Workflows** (state machines for long-running processes)

The FlowRule **Worker runtime** handles:
- Event consumption & deduplication
- Sharding & ordering guarantees
- Rule evaluation (deterministic, pure)
- Execution recording with decision hashes
- Outbox pattern for reliable effects
- Child event chaining
- Workflow state progression
- Retry, quarantine, observability

## Generated Types

Each example's `generated/` directory contains type-safe artifacts:

```
generated/
├── go/          # Go structs with validation tags
├── java/        # Java classes with Jackson/Jakarta annotations
├── protobuf/    # Protobuf messages
└── jsonschema/  # JSON Schema for validation
```

Use these directly in your application code - no manual DTO maintenance needed.

## Key Concepts Demonstrated

| Concept | E-commerce | Food Delivery | Banking |
|---------|------------|---------------|---------|
| Event chaining | ✅ | ✅ | ✅ |
| Conditional routing | ✅ | ✅ | ✅ |
| Sharding by key | ✅ (customer) | ✅ (postal code) | ✅ (account) |
| Workflow state machine | ✅ | ✅ | ✅ |
| Fraud/risk rules | ✅ | | ✅ |
| Geographic partitioning | | ✅ | |
| Compliance workflows | | | ✅ |
| Code generation | ✅ | ✅ | ✅ |

## Learn More

- [FlowRule Architecture](../ARCHITECTURE_MAP.md)
- [API Documentation](../docs/)
- [Rule Schema](../api/rule-schema.json)
- [Event Envelope Schema](../api/envelope-schema.json)