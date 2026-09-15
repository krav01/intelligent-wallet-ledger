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

curl http://localhost:8080/metrics | rg '^wallet_http_'
```

See [observability notes](docs/observability.md) for metric labels and a latency query.

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
  RISK_VELOCITY_REVIEW_TRANSFER_COUNT=3 \
  REDIS_ADDRESS='localhost:6379' \
  RISK_VELOCITY_WINDOW='5m' \
  make run-risk-worker
```

For a policy rotation, set `RISK_POLICIES_JSON` instead of the three single-policy
variables. It is a non-empty array of immutable USD policies, for example:

```bash
RISK_POLICIES_JSON='[{"version":"risk-v1","review_amount_usd_minor":50,"decline_amount_usd_minor":100,"velocity_review_transfer_count":3},{"version":"risk-v2","review_amount_usd_minor":75,"decline_amount_usd_minor":150,"velocity_review_transfer_count":3}]'
```

The worker selects the policy version captured in each request, so pending v1 events
remain reproducible while v2 is deployed.

When a policy enables velocity, `REDIS_ADDRESS` and `RISK_VELOCITY_WINDOW` are
required. The worker observes the source-account count over the configured sliding
window. Redis unavailability is captured and routed to deterministic manual review;
it never silently approves a transfer.

It commits a Kafka offset only after PostgreSQL atomically reserves the inbox event,
persists the assessment, transitions the transfer, and writes `transfer.risk_assessed`
to the outbox. Invalid or unknown events are first captured in an idempotent bounded
quarantine row; unavailable policy or infrastructure failures remain unacknowledged
for retry.

Run the complete worker topology and verify the terminal posting transitions:

```bash
make demo-transfer
```

The target migrates PostgreSQL, starts the outbox publisher plus both Kafka workers,
and runs tagged integration tests for the approved posting and insufficient-funds
failure paths. It does not claim an HTTP transfer command yet; that API surface is a
separate delivery slice.

To enable the analyst review command, configure one trusted OIDC issuer alongside
the wallet database. All OIDC settings are required together; without them the API
continues to expose only its operational endpoints.

```bash
DATABASE_URL='postgres://wallet:wallet_dev_only@localhost:5432/wallet?sslmode=disable' \
  OIDC_ISSUER='https://issuer.example' \
  OIDC_AUDIENCE='wallet-api' \
  OIDC_ROLE_CLAIM='roles' \
  REVIEW_DECISION_RATE_LIMIT_PER_MINUTE=12 \
  make run
```

An OIDC token containing the configured audience, a nonempty `sub`, and the
`analyst` role may decide an open review case:

```bash
curl -i -X POST 'http://localhost:8080/v1/transfers/TRANSFER_ID/review-decision' \
  -H 'Authorization: Bearer OIDC_TOKEN' \
  -H 'Content-Type: application/json' \
  --data '{"decision":"approved"}'
```

The endpoint returns `204 No Content`; it returns `401` for an absent or invalid
token, `403` for a verified principal without the analyst role, `404` for a missing
transfer, `409` when the review case is no longer open, and `429` when the verified
principal exceeds the command limit. `REVIEW_DECISION_RATE_LIMIT_PER_MINUTE` defaults
to 12 and accepts values from 1 through 600. The limiter is local to one wallet-api
process; use a shared limiter before deploying multiple replicas.

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

### Analyst review flow

```text
Analyst + Bearer JWT
        |
        v
wallet-api -- discovery / JWKS --> configured OIDC issuer
        |
        | verified subject + analyst role
        v
per-process principal rate limiter
        v
review-case transaction --> lifecycle + review case + audit record + outbox event
                                                                |
                                                                v
                                                        Kafka publisher
                                                                |
                                                                v
                                                     transaction worker --> ledger
```

The analyst endpoint is registered only when the OIDC issuer, audience, and role
claim are configured together. A decision is committed only with its audit record
and durable outbox event; a rejected or unavailable authentication provider does not
construct a trusted principal.

Core guarantees:

- the immutable double-entry ledger is the financial source of truth;
- every posted journal entry balances to zero per currency;
- balances are transactional snapshots updated with their ledger postings;
- financial mutation endpoints require idempotency keys;
- asynchronous consumers assume at-least-once delivery and are idempotent;
- AI is outside the money path and receives minimized, pseudonymous context.

See [architecture](docs/architecture.md), [roadmap](docs/roadmap.md),
[architecture decisions](docs/adr/README.md), and the
[threat model](docs/intelligent-wallet-ledger-threat-model.md), and the
[operational recovery runbook](docs/runbook.md). The machine-readable HTTP contract is
[OpenAPI 3.1](docs/openapi.yaml).

## Current status

Roadmap slices 1 through 11 are implemented, along with the threat model, operational
runbook, and Helm deployment artifacts from slice 12. The remaining slice-12 work is
measured k6 output. The Helm chart is linted and rendered in CI, but no Kubernetes
cluster deployment is claimed as complete.

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
Risk and transaction workers now advance accepted transfers through deterministic
assessment to an atomic ledger posting or an explicit insufficient-funds failure.

## Evidence map

| Claim | Reproduce it | Execution boundary |
| --- | --- | --- |
| Domain invariants, idempotency, authorization, and HTTP behavior remain deterministic. | `make test` | Local Go toolchain. |
| Concurrent code has no detected data race. | `make test-race` | Local Go toolchain. |
| PostgreSQL transactions, Kafka delivery, and worker recovery hold under real adapters. | `make test-integration` | Requires migrated PostgreSQL and Kafka; CI runs this suite. |
| A publisher crash after broker acknowledgement is safe for the consumer effect. | `make demo-outbox` | Requires Docker Compose; republished event is deduplicated by the inbox. |
| Approved and insufficient-funds transfer posting transitions are visible end to end. | `make demo-transfer` | Requires Docker Compose; starts publisher and both workers. |
| Known reachable dependency vulnerabilities and static issues are checked. | `make vuln`, `make lint`, `make vet` | CI also runs govulncheck, CodeQL, lint, and Go test matrix. |
| The public API stays available under a small repeatable workload. | `K6_BASE_URL=http://127.0.0.1:8080 make load-health` | k6 is required locally; CI runs 20 VUs for 15 seconds against its built API and logs the summary. |

The [k6 health workload](docs/load-testing.md) is executed in CI with a pinned k6
image version and reports its runner-specific summary in the job log. It is not a
financial-workflow capacity benchmark. The
[Helm chart](deploy/helm/intelligent-wallet-ledger/README.md) deploys the four process
workloads from immutable image digests and requires process-scoped existing Secrets.
Its CI validation is static; the [threat model](docs/intelligent-wallet-ledger-threat-model.md)
and [runbook](docs/runbook.md) remain the source of required cluster controls and
recovery procedure.

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
