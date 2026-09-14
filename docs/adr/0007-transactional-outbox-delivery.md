# ADR-0007: Transactional outbox and idempotent consumer delivery

- Status: Accepted
- Date: 2026-09-10

## Context

A committed transfer must eventually become visible to asynchronous workers without
making Kafka part of the financial transaction. Publishing after commit can lose an
event if the process crashes, while publishing before commit can expose a transfer
that later rolls back. Kafka retries and a crash after broker acknowledgement can also
deliver the same event more than once.

Events need an explicit compatibility contract and enough context for independent
workers. Consumers must not repeat durable side effects after redelivery. The design
must describe these guarantees honestly: broker idempotence can reduce duplicates,
but it cannot make the PostgreSQL-to-Kafka boundary exactly once.

## Decision

### Event creation and envelope

Persist one `transfer.completed` event only when a transfer is first committed. Write
the event in the same `SERIALIZABLE` PostgreSQL transaction as transfer metadata,
ledger rows, and balance snapshots. An idempotent replay returns the stored transfer
without inserting another event.

Use a versioned JSON envelope containing:

- an independent UUID `event_id`;
- `event_type` and positive integer `event_version` fields;
- `aggregate_type`, `aggregate_id`, and positive `aggregate_version` fields;
- `occurred_at`, `correlation_id`, and optional `causation_id` fields;
- an event-specific `payload` object.

The first contract is `transfer.completed` version 1. Its aggregate is the transfer,
its aggregate version is 1, its correlation ID is the transfer ID, and its occurrence
time is the transfer request time. The payload contains the canonical requester,
source account, destination account, currency, minor units, and request time. UUIDs
act as pseudonymous identifiers; the event contains no names, credentials, balances,
free-form text, or AI data.

Publish envelopes to `wallet.events.v1` with `transfer:<transfer_id>` as the Kafka
record key. The versioned topic identifies the broad compatibility generation;
`event_type` and `event_version` select the concrete decoder. Before an aggregate has
multiple event types, ordering beyond Kafka's per-partition record order is not a
claimed guarantee.

### Outbox publication

Store the envelope and delivery state in a generic PostgreSQL outbox. Each row has a
UUID event ID, a monotonic database sequence for scan order, envelope metadata,
`JSONB` payload, availability and creation times, attempt count, a bounded claim
lease, last sanitized error, and nullable publication time. A partial index supports
finding available unpublished rows in sequence order. Aggregate identifiers are not
foreign keys because the outbox is shared by bounded contexts and retained rows must
not control aggregate deletion semantics.

A transaction-scoped outbox writer accepts an existing `pgx.Tx`; it never begins,
commits, rolls back, or retries. The transfer repository remains the owner of the
financial transaction and its complete retry loop.

Publishers atomically claim a bounded batch with `FOR UPDATE SKIP LOCKED`, commit the
claim, and perform Kafka I/O outside the database transaction. A claim has an expiry
so another publisher can recover work after a crash. After broker acknowledgement,
the claimant marks the event published. Failures clear or expire the claim, increment
the attempt count, store a bounded sanitized diagnostic, and schedule retry with
bounded exponential backoff.

Use Kafka acknowledgement of all in-sync replicas and an idempotent producer. A crash
after Kafka acknowledgement but before `published_at` is committed republishes the
event. This duplicate is expected and is handled by consumer inboxes. Published rows
are retained for audit and recovery in this slice; retention and archival require a
later measured policy.

### Consumer inbox

Identify a consumer by a stable bounded name. Before applying a durable PostgreSQL
side effect, insert `(consumer_name, event_id)` into a generic inbox in the same
database transaction as that side effect. If the key already exists, make delivery a
no-op. Commit the Kafka offset only after the database transaction commits.

Envelope validation, event-version dispatch, and handler errors occur before offset
commit. A valid event owned by another known worker is acknowledged as ignored. An
invalid envelope, invalid payload for an owned event type, or unknown event type is
stored idempotently in PostgreSQL quarantine before its offset is committed. The row
records consumer/topic/partition/offset plus a SHA-256 fingerprint and a value excerpt
bounded to 64 KiB; a failed quarantine write remains retryable. Operators recover a
larger source record from Kafka by the stored position before broker retention expires
and republish a corrected event with a new event ID. Handler infrastructure failures
and unavailable policy remain uncommitted. Side effects in systems other than
PostgreSQL need their own idempotency key or reconciliation strategy and are not
covered by the inbox transaction.

## Consequences

Financial state and event intent cannot diverge at commit. Publisher crashes cannot
lose claimed work, and duplicate Kafka delivery does not repeat PostgreSQL consumer
effects. Publication is at least once, not exactly once; duplicates remain observable
and expected. The lease state makes the publisher more complex but avoids holding a
database transaction and connection open during network I/O.

The JSON envelope is stable and evolvable without coupling consumers to Go structs or
database rows. Event payloads duplicate selected transfer data so consumers need not
read another bounded context synchronously. Published outbox retention consumes
storage until a later archival policy is accepted.

## Rejected alternatives

- Publish directly after the transfer commit: a crash can permanently lose the event.
- Publish before committing PostgreSQL: consumers can observe rolled-back money state.
- Hold row locks while calling Kafka: simpler claiming, but network stalls consume
  database connections and extend lock lifetimes.
- Claim end-to-end exactly-once delivery: PostgreSQL and Kafka do not share one atomic
  commit, so the claim would be false even with Kafka producer idempotence.
- Put business side effects in database triggers: hidden behavior violates the
  repository's explicit transaction boundary.

## Revisit when

Revisit ordering before emitting multiple events for one aggregate, quarantine
retention and event-contract ownership after measuring event volume, and the payload
privacy boundary before adding identity or AI-related events. Revisit the inbox model
before a consumer performs non-PostgreSQL side effects.
