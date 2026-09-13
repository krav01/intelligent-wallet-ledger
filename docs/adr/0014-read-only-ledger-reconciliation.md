# ADR-0014: Read-only ledger reconciliation

- Status: Accepted
- Date: 2026-09-13

## Decision

Reconciliation reads account balance snapshots and immutable posting totals in one
repeatable-read, read-only PostgreSQL transaction. It returns only discrepancies and
never repairs data. Ledger totals and snapshots are represented as decimal strings in
the report so a damaged ledger total cannot overflow a diagnostic `BIGINT` value.

## Consequences

Operators can detect snapshot drift without a reconciliation run changing financial
state. Correction remains an explicit, separately authorized operation with an audit
record. The initial report is repository-level; scheduling, alerts, and remediation
are deferred.

## Revisit triggers

Revisit before automated remediation, high-volume report streaming, or a deployed
reconciliation worker.
