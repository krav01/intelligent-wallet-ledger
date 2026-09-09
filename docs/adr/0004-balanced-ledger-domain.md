# ADR-0004: Balanced immutable ledger domain

- Status: Accepted
- Date: 2026-09-10

## Context

The ledger needs an in-memory domain boundary that rejects malformed financial
entries before persistence is involved. A valid model must support multiple
currencies without allowing amounts in one currency to offset another, preserve
posted history, and make every accepted entry reversible.

## Decision

Represent a journal entry as an immutable identifier, recording time, type, and a
defensively copied list of 2 to 1024 postings. Each posting identifies one account and
contains a non-zero signed `Money` amount. Positive amounts increase an account
snapshot and negative amounts decrease it.

Require postings to sum to exactly zero independently for every currency. Aggregate
minor units with arbitrary-precision integers during validation so the result is not
affected by posting order or intermediate `int64` overflow. Reject `math.MinInt64` as
a posting amount because its opposite is not representable and would make the entry
impossible to reverse.

Corrections create a new reversal entry with the same accounts and currencies and
opposite amounts. The reversal records the identifier of the entry it corrects; the
original entry remains unchanged. Persistence will later enforce immutability and
whether an entry may have more than one reversal.

## Consequences

The domain rejects cross-currency netting, zero movements, unbounded entry sizes, and
entries that cannot be corrected. Exact validation allocates a small accumulator per
currency, bounded by the posting limit. Reversals remain explicit audit records
instead of hidden updates.

This decision does not yet make the ledger durable. PostgreSQL tables, transactional
balance updates, and database constraints remain part of the next ledger slice.

## Revisit when

Revisit the posting limit using measured production workloads, or when compound
financial products require richer posting metadata. Any change to sign semantics,
per-currency balancing, or append-only reversals requires a superseding ADR and
migration-compatible tests.
