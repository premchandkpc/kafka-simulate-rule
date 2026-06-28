RUST_DIR := rust
GO_PKG   := ./go/cmd/flowrule
BIN      := flowrule
CGO      := CGO_ENABLED=1

.PHONY: all rust go test clean

all: rust go

rust:
	cd $(RUST_DIR) && cargo build --release

go: rust
	$(CGO) go build -o $(BIN) $(GO_PKG)

test-rust:
	cd $(RUST_DIR) && cargo test

test-go:
	$(CGO) go test ./go/... -v -count=1

test: test-rust test-go

vet:
	$(CGO) go vet ./go/...

clean:
	cd $(RUST_DIR) && cargo clean
	rm -f $(BIN)
