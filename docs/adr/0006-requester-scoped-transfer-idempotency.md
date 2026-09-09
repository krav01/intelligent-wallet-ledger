# ADR-0006: Requester-scoped transfer idempotency

- Status: Proposed
- Date: 2026-09-10

## Context

Clients can retry a transfer after a timeout or lost response. A check performed
outside the ledger transaction can race and apply the same intent twice. The key also
needs a namespace so unrelated callers cannot collide globally, and a replay with
different financial intent must not silently return an unrelated result.

## Proposed decision

Scope each idempotency key by the authenticated requester. Normalize it by trimming
edge whitespace, then require 1 to 128 visible ASCII bytes and preserve its remaining
case-sensitive value. Compare canonical client intent using requester, source account,
destination account, currency, and minor units. Exclude server-generated transfer IDs
and timestamps from that comparison.

Use the transfer ID as its journal entry ID. A transfer maps to exactly two ordered
postings: an equal source debit and destination credit in one currency. The source
account must be an active customer account owned by the requester; the destination
must be a distinct active customer account in the same currency. System accounts stay
outside the customer transfer path.

The transfer operation owns one `SERIALIZABLE` transaction covering idempotency,
transfer metadata, ledger rows, and balance snapshots. Any serialization or deadlock
retry restarts that whole transaction. The database unique constraint on
`(requester_id, idempotency_key)` resolves concurrent first use.

The same scoped key and same canonical intent returns the originally committed
transfer without another balance effect. The same key with different intent returns a
stable conflict. This slice stores only committed successes; validation, not-found,
currency, and insufficient-funds failures roll back and may be retried later with the
same key.

## Consequences

Successful replay is deterministic, a lost response can be recovered by key, and the
ledger effect cannot diverge from transfer metadata. Failed attempts are not durable,
so their result can change after account state changes. The future asynchronous
lifecycle may add durable pending or declined states without changing successful-key
semantics.

## Revisit when

Revisit failure-result persistence when asynchronous processing is introduced, key
retention when API usage is measured, and the requester scope if service accounts or
delegated authorization require a different namespace.
