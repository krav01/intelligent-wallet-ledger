# ADR-0003: Wallet, account, and balance snapshot schema

- Status: Accepted
- Date: 2026-09-09

## Context

The wallet context needs currency-specific accounts and fast balance reads before the
immutable ledger is introduced. A snapshot must not become an independent financial
source of truth or permit currencies to diverge between an account and its balance.

## Decision

Store wallets, accounts, and account balance snapshots in separate PostgreSQL tables.
A wallet may own at most one account per currency. A composite foreign key from
`account_balances(account_id, currency)` to `accounts(id, currency)` enforces currency
agreement. Customer balance snapshots and their versions must be non-negative.

Create a wallet, all requested currency accounts, and their zero-value snapshots in a
single `SERIALIZABLE` transaction. Read the aggregate in a read-only `REPEATABLE READ`
transaction. Limit creation to 32 currencies per wallet so one request cannot create
an unbounded transaction. The wallet repository exposes no balance mutation method;
a later ledger posting transaction will be the only writer.

Use `golang-migrate` for versioned up/down migrations and `pgx` for explicit,
parameterized PostgreSQL access.

## Consequences

Wallet creation is atomic and deterministic, and readers cannot observe accounts
without their snapshots. Currency mismatches and negative customer snapshots fail at
the database boundary. Serializable creation costs more than read committed but the
operation is infrequent and establishes several financial records at once.

The snapshot is deliberately denormalized for efficient reads. Reconciliation must
later prove that it matches immutable ledger postings.

## Revisit when

Revisit transaction isolation after measured contention, when product requirements
permit multiple accounts for the same wallet and currency, or when overdraft products
require negative customer snapshots. Any relaxation requires ledger and reconciliation
tests plus a superseding ADR.
