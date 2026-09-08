# Delivery roadmap

Each slice should remain independently reviewable and merge only after its relevant
local checks and required CI pass. Neighboring slices may be combined when their diff
is small and coherent.

1. Repository foundation: process lifecycle, docs, CI, Docker Compose, health checks.
2. Money and currency value objects with property and fuzz tests.
3. Wallets, accounts, PostgreSQL migrations, and transactional balance snapshots.
4. Immutable ledger with balanced postings and reversal entries.
5. Transfers, idempotency keys, stable account locking, and concurrency tests.
6. Transactional outbox, Kafka publisher, event envelopes, and consumer inboxes.
7. Asynchronous transfer lifecycle and deterministic risk engine.
8. Redis velocity signals, manual-review cases, and explicit degraded-mode policy.
9. AI Investigator port, mock provider, privacy boundary, and provider-failure tests.
10. Reconciliation, audit trail, RBAC, and API abuse protection.
11. OpenTelemetry, Prometheus/Grafana, failure injection, and recovery tests.
12. k6 measurements, Kubernetes/Helm, threat model, runbooks, and portfolio polish.

The first public demo is complete when it visibly proves balanced postings, safe
idempotent replay, no overdraft under concurrent withdrawals, deterministic review of
an anomalous transfer, AI explanation without money-path authority, and successful
reconciliation.
