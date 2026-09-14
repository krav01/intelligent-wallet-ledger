# Delivery roadmap

Each slice should remain independently reviewable and merge only after its relevant
local checks and required CI pass. Neighboring slices may be combined when their diff
is small and coherent.

1. [x] Repository foundation: process lifecycle, docs, CI, Docker Compose, health checks.
2. [x] Money and currency value objects with property and fuzz tests.
3. [x] Wallets, accounts, PostgreSQL migrations, and transactional balance snapshots.
4. [x] Immutable ledger with balanced postings and reversal entries.
   - [x] Pure domain model with exact per-currency balancing and reversals.
   - [x] PostgreSQL persistence, constraints, and atomic balance updates.
5. [x] Transfers, idempotency keys, stable account locking, and concurrency tests.
   - [x] Pure transfer intent with positive amounts, distinct accounts, and balanced postings.
   - [x] Atomic PostgreSQL idempotency, ledger composition, and replay/concurrency tests.
6. [x] Transactional outbox, Kafka publisher, event envelopes, and consumer inboxes.
   - [x] Versioned event envelope plus PostgreSQL outbox and consumer inbox foundation.
   - [x] Kafka publisher, redelivery proof, and local end-to-end demo.
7. Asynchronous transfer lifecycle and deterministic risk engine.
	- [x] Pure lifecycle state machine and versioned deterministic risk policy.
	- [x] Reviewed persistence migration, workers, event contracts, and worker topology demo.
8. Redis velocity signals, manual-review cases, and explicit degraded-mode policy.
	- [x] Captured velocity input, Redis sliding-window observation, and fail-safe review.
	- [x] Durable review cases linked atomically to review-required assessments.
	- [x] Analyst decision authorization through a trusted-principal application boundary.
9. [x] AI Investigator port, mock provider, privacy boundary, and provider-failure tests.
10. Reconciliation, audit trail, RBAC, and API abuse protection.
	- [x] Read-only ledger reconciliation report for balance snapshot drift.
	- [x] Atomic audit trail for analyst review decisions.
	- [x] HTTP request-header size limit for abuse protection.
	- [x] OIDC analyst role enforcement and per-principal command-level API abuse protection.
11. OpenTelemetry, Prometheus/Grafana, failure injection, and recovery tests.
	- [x] Bounded Prometheus HTTP traffic and latency metrics.
	- [x] OpenTelemetry HTTP trace-context propagation.
	- [x] Transaction-worker rollback and redelivery after a recoverable posting failure.
12. k6 measurements, Kubernetes/Helm, threat model, runbooks, and portfolio polish.
	- [x] Repository-grounded threat model with deployment assumptions and review paths.
	- [x] Operational recovery runbook for dependencies, delivery, and ledger drift.
	- [x] README evidence map that separates verified claims from unimplemented deployment work.
	- [x] Digest-pinned Helm workloads with CI rendering and documented deployment boundaries.

The first public demo is complete when it visibly proves balanced postings, safe
idempotent replay, no overdraft under concurrent withdrawals, deterministic review of
an anomalous transfer, AI explanation without money-path authority, and successful
reconciliation.
