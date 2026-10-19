GO ?= $(shell command -v go 2>/dev/null || echo /opt/homebrew/bin/go)

.PHONY: build test lint
build:
	$(GO) build -o bin/trib ./cmd/trib
test:
	$(GO) test ./...
lint:
	$(GO) vet ./...
