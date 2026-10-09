.PHONY: help build test test-unit test-integration test-e2e lint vet fmt generate clean run-api run-worker run-all docker-build docker-up docker-down migrate-up migrate-down proto-gen proto-validate

# Default target
help:
	@echo "FlowRule - Event-driven Rules Engine"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@echo "Build targets:"
	@echo "  build           Build all binaries"
	@echo "  build-api       Build API server binary"
	@echo "  build-worker    Build worker binary"
	@echo "  build-codegen   Build codegen binary"
	@echo ""
	@echo "Test targets:"
	@echo "  test            Run all tests"
	@echo "  test-unit       Run unit tests only"
	@echo "  test-integration Run integration tests"
	@echo "  test-e2e        Run end-to-end tests (requires infrastructure)"
	@echo ""
	@echo "Code quality:"
	@echo "  lint            Run golangci-lint"
	@echo "  vet             Run go vet"
	@echo "  fmt             Format code with gofmt"
	@echo ""
	@echo "Code generation:"
	@echo "  generate        Generate all code from contracts"
	@echo "  proto-gen       Generate protobuf and gRPC code"
	@echo "  proto-validate  Validate protobuf files with protoc"
	@echo ""
	@echo "Run targets:"
	@echo "  run-api         Run API server"
	@echo "  run-worker      Run worker"
	@echo "  run-all         Run API, worker, and dependencies via docker-compose"
	@echo ""
	@echo "Docker targets:"
	@echo "  docker-build    Build Docker images"
	@echo "  docker-up       Start all services with docker-compose"
	@echo "  docker-down     Stop all services"
	@echo "  docker-logs     View docker-compose logs"
	@echo ""
	@echo "Database:"
	@echo "  migrate-up      Run database migrations"
	@echo "  migrate-down    Rollback last migration"
	@echo ""
	@echo "Utilities:"
	@echo "  clean           Clean build artifacts"
	@echo "  deps            Download and verify dependencies"
	@echo "  tools           Install development tools"

# Build variables
BIN_DIR := bin
API_BINARY := $(BIN_DIR)/api
WORKER_BINARY := $(BIN_DIR)/worker
CODEGEN_BINARY := $(BIN_DIR)/codegen
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME)"

# Go variables
GO := go
GOFLAGS := -trimpath
GOTESTFLAGS := -race -count=1

# Build all binaries
build: build-api build-worker build-codegen

build-api:
	@echo "Building API server..."
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(API_BINARY) ./cmd/api

build-worker:
	@echo "Building worker..."
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(WORKER_BINARY) ./cmd/worker

build-codegen:
	@echo "Building codegen..."
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(CODEGEN_BINARY) ./cmd/codegen

# Run targets
run-api: build-api
	@echo "Starting API server..."
	$(API_BINARY)

run-worker: build-worker
	@echo "Starting worker..."
	$(WORKER_BINARY)

# Run with docker-compose
run-all: docker-up
	@echo "Services started. API at http://localhost:8080, gRPC at localhost:9090"
	@echo "Run 'make docker-logs' to view logs"
	@echo "Run 'make docker-down' to stop"

# Test targets
test: test-unit test-integration

test-unit:
	@echo "Running unit tests..."
	$(GO) test $(GOTESTFLAGS) ./internal/...

test-integration:
	@echo "Running integration tests..."
	$(GO) test $(GOTESTFLAGS) -tags=integration ./tests/...

test-e2e:
	@echo "Running end-to-end tests..."
	$(GO) test $(GOTESTFLAGS) -tags=e2e ./tests/e2e/...

# Code quality
lint:
	@echo "Running golangci-lint..."
	@which golangci-lint > /dev/null || (echo "golangci-lint not installed. Run 'make tools'" && exit 1)
	golangci-lint run ./...

vet:
	@echo "Running go vet..."
	$(GO) vet ./...

fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...

# Code generation
generate:
	@echo "Generating code from contracts..."
	$(GO) run ./cmd/codegen generate --all

proto-gen:
	@echo "Generating protobuf and gRPC code..."
	$(GO) run ./cmd/codegen generate --proto --grpc

proto-validate:
	@echo "Validating protobuf files..."
	@which protoc > /dev/null || (echo "protoc not installed" && exit 1)
	find . -name "*.proto" -exec protoc --proto_path=. --go_out=. --go-grpc_out=. {} \;

# Docker targets
docker-build:
	@echo "Building Docker images..."
	docker-compose build

docker-up:
	@echo "Starting services..."
	docker-compose up -d

docker-down:
	@echo "Stopping services..."
	docker-compose down

docker-logs:
	docker-compose logs -f

# Database migrations
migrate-up:
	@echo "Running migrations..."
	$(GO) run ./cmd/migrate up

migrate-down:
	@echo "Rolling back migration..."
	$(GO) run ./cmd/migrate down

# Utilities
clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(BIN_DIR)
	rm -rf /tmp/flowrule-generated
	$(GO) clean -cache -testcache -modcache

deps:
	@echo "Downloading dependencies..."
	$(GO) mod download
	$(GO) mod verify

tools:
	@echo "Installing development tools..."
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	$(GO) install github.com/pressly/goose/v3/cmd/goose@latest

# Generate example contracts
generate-examples:
	@echo "Generating code for banking example..."
	cd examples/banking && $(GO) run ../../cmd/codegen generate --contract transaction.initiated --version 1.0 --output ./generated --all

# Verify generated code compiles
verify-generated:
	@echo "Verifying generated code compiles..."
	@for dir in examples/*/generated/go; do \
		if [ -d "$$dir" ]; then \
			echo "Checking $$dir..."; \
			cd $$dir && go build ./... || exit 1; \
			cd - > /dev/null; \
		fi; \
	done

# CI pipeline
ci: deps fmt vet lint test-unit verify-generated
	@echo "CI pipeline passed!"

# Release
release: ci build docker-build
	@echo "Release artifacts ready in $(BIN_DIR)/"