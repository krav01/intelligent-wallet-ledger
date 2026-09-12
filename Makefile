BINARY := bin/wallet-api
PUBLISHER_BINARY := bin/outbox-publisher
RISK_WORKER_BINARY := bin/risk-worker
TRANSACTION_WORKER_BINARY := bin/transaction-worker
GO ?= go

.PHONY: build clean demo-outbox fmt help migrate-down migrate-up run run-publisher run-risk-worker run-transaction-worker test test-integration test-race vet vuln

## build: Build the wallet API.
build:
	$(GO) build -trimpath -o $(BINARY) ./cmd/wallet-api
	$(GO) build -trimpath -o $(PUBLISHER_BINARY) ./cmd/outbox-publisher
	$(GO) build -trimpath -o $(RISK_WORKER_BINARY) ./cmd/risk-worker
	$(GO) build -trimpath -o $(TRANSACTION_WORKER_BINARY) ./cmd/transaction-worker

## clean: Remove local build and coverage artifacts.
clean:
	rm -rf bin coverage.out coverage.html

## demo-outbox: Prove safe Kafka redelivery and an exactly-once consumer side effect.
demo-outbox:
	docker compose up -d postgres kafka
	$(MAKE) migrate-up
	TEST_DATABASE_URL='postgres://wallet:wallet_dev_only@localhost:5432/wallet?sslmode=disable' \
		KAFKA_BROKERS='localhost:9092' \
		$(GO) test -count=1 -v -tags=integration \
		-run '^TestPublisherRedeliveryKeepsEventIdentityAndConsumerEffectOnce$$' ./internal/outbox

## fmt: Format all Go packages.
fmt:
	$(GO) fmt ./...

## lint: Run the configured linters.
lint:
	golangci-lint run ./...

## migrate-up: Apply all PostgreSQL migrations through Docker Compose.
migrate-up:
	docker compose run --rm migrate

## migrate-down: Revert the latest PostgreSQL migration through Docker Compose.
migrate-down:
	docker compose run --rm migrate -path=/migrations -database="postgres://wallet:wallet_dev_only@postgres:5432/wallet?sslmode=disable" down 1

## run: Run the wallet API.
run:
	$(GO) run ./cmd/wallet-api

## run-publisher: Run the PostgreSQL outbox publisher.
run-publisher:
	$(GO) run ./cmd/outbox-publisher

## run-risk-worker: Run the deterministic Kafka risk worker.
run-risk-worker:
	$(GO) run ./cmd/risk-worker

## run-transaction-worker: Run the Kafka transaction posting worker.
run-transaction-worker:
	$(GO) run ./cmd/transaction-worker

## test: Run deterministic unit tests.
test:
	$(GO) test -shuffle=on -cover ./...

## test-integration: Run tests that require TEST_DATABASE_URL and migrated PostgreSQL.
test-integration:
	$(GO) test -tags=integration -shuffle=on ./...

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
