# ADR-0005: Transactional ledger persistence and account types

- Status: Accepted
- Date: 2026-09-10

## Context

Persisting a balanced journal entry must not expose postings without their balance
effects or update snapshots without durable ledger evidence. Customer accounts must
remain nonnegative, but an initially empty double-entry system also needs a settlement
side that may carry a negative balance. Duplicate postings for one account and
concurrent entries must not make results depend on posting order.

## Decision

Add `customer` and `system` account types. Wallet creation continues to create only
customer accounts. Customer balance snapshots remain nonnegative; system accounts may
be negative and provide the clearing or settlement side. System-account provisioning
is a controlled platform operation and is not exposed by the wallet API in this slice.

Store immutable journal entries and position-ordered postings in PostgreSQL. A
reversal references its source, must be its exact positional inverse, and each source
may have at most one reversal. Foreign keys use restrictive deletion so wallet or
account deletion cannot erase financial history. Cross-row balancing remains a domain
and repository invariant because this project avoids hidden trigger logic; reads
reconstruct entries through domain constructors and reject corrupt rows.

Post an entry in one `SERIALIZABLE` transaction. Aggregate duplicate postings per
account with exact arithmetic, lock distinct balance rows in canonical UUID order,
validate currency and checked `BIGINT` boundaries, insert the entry and postings, then
update every participating account snapshot once. A zero-net participating account
still increments its version once. Retry the entire transaction at most three times
after serialization failures or deadlocks.

## Consequences

Ledger rows and snapshots commit or roll back together, concurrent posting has a
stable lock order, and intermediate posting order cannot cause a false overdraft or
overflow. Database constraints back individual-row shape, account currency agreement,
customer nonnegativity, and one-reversal enforcement.

A reversal can be mathematically valid but fail to post if a customer has already
spent the credited funds. The original history remains unchanged; a later operations
workflow must resolve the deficit through a reviewed compensating entry. Database
owners remain trusted because application-level immutability is not implemented with
triggers or a separate append-only database role yet.

## Revisit when

Revisit system-account provisioning before exposing funding APIs, retry limits after
measuring contention, and database roles before production deployment. Revisit the
reversal failure workflow when manual-review and reconciliation slices are added.
