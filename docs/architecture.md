# Architecture

## Purpose

`intelligent-wallet-ledger` demonstrates financial correctness, concurrency control,
event-driven recovery, deterministic fraud assessment, and a constrained AI assistant
in one reviewable Go repository.

## System shape

The system uses modular boundaries with multiple deployable binaries rather than a
microservice per package. Planned applications are `wallet-api`, `risk-worker`,
`transaction-worker`, `projection-worker`, `reconciliation-worker`, and
`ai-investigator`.

Bounded contexts are identity, wallet, transfer, ledger, risk, investigation,
reconciliation, audit, and platform. Domain packages do not import HTTP, Kafka,
Redis, PostgreSQL, or an LLM SDK. Composition roots use manual constructor injection.

## Financial source of truth

The ledger stores immutable journal entries and postings. Money is represented in
minor units with an explicit currency. A journal entry may be posted only when its
postings sum to zero independently for every currency.

The ledger domain owns immutable `Currency` and `Money` value objects. Currency
codes use strict three-letter uppercase ASCII representation; the application layer
may further restrict the product's supported currencies. Signed `int64` minor units
support debit and credit postings. Arithmetic rejects invalid values, mixed
currencies, and integer overflow.

`account_balances` is a transactional read model, not an independent source of truth.
Posting a transaction locks affected accounts in stable order and atomically writes
the journal entry, postings, balance snapshots, and outbox events. Corrections append
reversal entries instead of mutating posted history.

## Asynchronous delivery

Database state and outbox events are committed in one PostgreSQL transaction. Kafka
provides at-least-once transport; consumers persist `(event_id, consumer_name)` and
must make duplicate delivery a no-op. Event envelopes carry event, aggregate,
correlation, and causation identifiers.

## Risk and AI boundary

The risk engine is deterministic and produces a score, signals, and one of approve,
review, or decline. Redis may hold ephemeral velocity counters; durable assessments
and cases live in PostgreSQL. A Redis failure follows an explicit fail-safe policy for
high-risk operations and cannot corrupt the ledger.

The AI Investigator runs only after a review case exists. It receives minimized,
pseudonymous structured context and returns an explanation for a human analyst. Its
output is never consumed by a command that changes financial state. AI unavailability
degrades investigation only.

## Verification strategy

Domain tests prove balancing and idempotency invariants. Integration tests exercise
PostgreSQL transactions, outbox publication, and consumer deduplication. Concurrency
tests attempt double-spend and lock-order failures. Recovery tests inject crashes and
redelivery. Published load-test claims must include the workload, environment, and raw
results.
