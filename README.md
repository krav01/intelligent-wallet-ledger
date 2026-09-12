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

The portfolio checks prove, with tests and observable output, that duplicate requests
do not double-spend, concurrent withdrawals cannot overdraw an account, and Kafka
redelivery does not repeat a consumer's database effect. Later slices add visible
reconciliation and AI-outage proofs.

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

Run the PostgreSQL and Kafka adapter tests against the migrated local services:

```bash
TEST_DATABASE_URL='postgres://wallet:wallet_dev_only@localhost:5432/wallet?sslmode=disable' \
  KAFKA_BROKERS='localhost:9092' \
  make test-integration
```

Run the focused outbox demo:

```bash
make demo-outbox
```

The demo deliberately simulates a publisher crash after Kafka acknowledges a record
but before PostgreSQL records completion. It then republishes the same event and
verifies that the consumer inbox applies the database side effect once.

Run the deterministic risk worker with an explicit, versioned policy:

```bash
DATABASE_URL='postgres://wallet:wallet_dev_only@localhost:5432/wallet?sslmode=disable' \
  KAFKA_BROKERS='localhost:9092' \
  RISK_POLICY_VERSION='risk-v1' \
  RISK_REVIEW_AMOUNT_USD_MINOR=50 \
  RISK_DECLINE_AMOUNT_USD_MINOR=100 \
  make run-risk-worker
```

For a policy rotation, set `RISK_POLICIES_JSON` instead of the three single-policy
variables. It is a non-empty array of immutable USD policies, for example:

```bash
RISK_POLICIES_JSON='[{"version":"risk-v1","review_amount_usd_minor":50,"decline_amount_usd_minor":100},{"version":"risk-v2","review_amount_usd_minor":75,"decline_amount_usd_minor":150}]'
```

The worker selects the policy version captured in each request, so pending v1 events
remain reproducible while v2 is deployed.

It commits a Kafka offset only after PostgreSQL atomically reserves the inbox event,
persists the assessment, transitions the transfer, and writes `transfer.risk_assessed`
to the outbox. A malformed event or unavailable policy is not acknowledged and is
delivered again for operator recovery.

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

Roadmap slices 1 through 6 are implemented: repository foundation, immutable money
value objects, PostgreSQL-backed wallets, the transactional double-entry ledger, and
requester-scoped idempotent customer transfers.
Ledger posting locks accounts in stable order and atomically stores entries, postings,
balance snapshots, transfer metadata, and a versioned `transfer.completed` outbox event.
Successful transfer retries return the original result without another balance effect
or event, while changed intent conflicts.
Customer accounts remain nonnegative; controlled system accounts provide the
settlement side. Unit and tagged integration tests cover domain, persistence,
atomicity, replay, authorization, reversal, overflow, outbox leasing/fencing,
consumer deduplication, and concurrency boundaries. A dedicated process publishes
the durable outbox to Kafka with stable aggregate keys and all-ISR acknowledgements;
the integration suite proves safe redelivery after a lost completion update.

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
