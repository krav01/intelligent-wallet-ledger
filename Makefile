BINARY := bin/wallet-api
GO ?= go

.PHONY: build clean fmt help lint run test test-race vet vuln

## build: Build the wallet API.
build:
	$(GO) build -trimpath -o $(BINARY) ./cmd/wallet-api

## clean: Remove local build and coverage artifacts.
clean:
	rm -rf bin coverage.out coverage.html

## fmt: Format all Go packages.
fmt:
	$(GO) fmt ./...

## lint: Run the configured linters.
lint:
	golangci-lint run ./...

## run: Run the wallet API.
run:
	$(GO) run ./cmd/wallet-api

## test: Run deterministic unit tests.
test:
	$(GO) test -shuffle=on -cover ./...

## test-race: Run unit tests with the race detector.
test-race:
	$(GO) test -race -shuffle=on ./...

## vet: Run Go's built-in static analysis.
vet:
	$(GO) vet ./...

## vuln: Scan reachable code for known vulnerabilities.
vuln:
	govulncheck ./...

## help: Show available targets.
help:
	@sed -n 's/^## /  /p' $(MAKEFILE_LIST)
