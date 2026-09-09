# intelligent-wallet-ledger

[![Go Version](https://img.shields.io/github/go-mod/go-version/krav01/intelligent-wallet-ledger)](https://go.dev/) [![CI](https://img.shields.io/github/actions/workflow/status/krav01/intelligent-wallet-ledger/test.yml?branch=main)](https://github.com/krav01/intelligent-wallet-ledger/actions) [![License](https://img.shields.io/github/license/krav01/intelligent-wallet-ledger)](./LICENSE)

A production-oriented educational platform for wallets, transfers, an immutable
double-entry ledger, deterministic fraud assessment, and AI-assisted investigation.
It is a portfolio system, not a bank and not an integration with real money.

Financial state is always determined by domain invariants and PostgreSQL transactions.
The AI Investigator explains an already-computed risk decision to a human analyst; it
cannot approve, decline, post, reverse, or otherwise mutate a financial operation.

## Demo

The first foundation slice exposes operational endpoints:

```bash
make run
curl http://localhost:8080/healthz
# {"status":"ok"}
```

The target portfolio demo will prove, with tests and observable output, that duplicate
requests do not double-spend, concurrent withdrawals cannot overdraw an account,
Kafka redelivery is safe, reconciliation detects mismatches, and AI outages do not
affect payment processing.

## Getting started

Requirements: Go 1.26 or Docker with Compose.

```bash
cp .env.example .env
make test
make run
```

To start the API together with PostgreSQL, Kafka, and Redis:

```bash
docker compose up -d --build
make migrate-up
curl http://localhost:8080/readyz
```

Run the PostgreSQL adapter tests against the migrated local database:

```bash
TEST_DATABASE_URL='postgres://wallet:wallet_dev_only@localhost:5432/wallet?sslmode=disable' \
  make test-integration
```

## Architecture

The repository is a modular Go system with small deployable applications. Bounded
contexts own their domain logic and expose narrow application ports; infrastructure
adapters depend inward on those ports.

```text
Client -> Wallet API -> PostgreSQL + transactional outbox -> Kafka
                                                        |-> Risk worker -> Redis
                                                        |-> Transaction worker -> Ledger

Risk signals + prepared history -> AI Investigator -> explanation for analyst
```

Core guarantees:

- the immutable double-entry ledger is the financial source of truth;
- every posted journal entry balances to zero per currency;
- balances are transactional snapshots updated with their ledger postings;
- financial mutation endpoints require idempotency keys;
- asynchronous consumers assume at-least-once delivery and are idempotent;
- AI is outside the money path and receives minimized, pseudonymous context.

See [architecture](docs/architecture.md), [roadmap](docs/roadmap.md), and
[architecture decisions](docs/adr/README.md).

## Current status

Roadmap slices 1 through 3 are implemented: repository foundation, immutable money
value objects, and PostgreSQL-backed wallets with one account per currency. The first
part of slice 4 adds immutable journal entries, exact per-currency balancing, and
append-only reversal entries. PostgreSQL ledger persistence and atomic balance updates
remain pending. Unit and tagged integration tests cover the implemented domain and
persistence boundaries.

## Development

```bash
make fmt
make vet
make test-race
make test-integration
make lint
make vuln
make build
```

## Contributing

Keep changes aligned with one roadmap slice and preserve the invariants in
[AGENTS.md](AGENTS.md). Every material architecture change requires an ADR.

## License

This project is licensed under the [MIT License](LICENSE).
