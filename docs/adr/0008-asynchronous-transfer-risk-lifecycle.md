# ADR-0008: Asynchronous transfer lifecycle and deterministic risk assessment

- Status: Accepted
- Date: 2026-09-11

## Context

Transfers are currently persisted only after their ledger effect has committed. The
transfer ID is also the journal entry ID, and the schema requires that journal entry
to exist before the transaction commits. This correctly models completed transfers
but cannot represent a request waiting for risk assessment, a deterministic decline,
or a posting failure.

The next delivery slice introduces asynchronous risk assessment before money moves.
Kafka delivery is at least once, so every state transition can be requested more than
once. Risk policy can change between deployments, but retrying one accepted transfer
must not silently evaluate it under a different policy. Account state can also change
while assessment is in progress, so a risk approval cannot reserve funds or promise
that ledger posting will succeed.

The design must preserve the financial authority boundary in ADR-0002: deterministic
code may decide and post, while AI remains advisory and outside the money path.

## Decision

### Transfer aggregate and lifecycle

Evolve the existing transfer record into the durable aggregate rather than creating a
parallel request table. Separate immutable intent from mutable lifecycle data. Keep
the server-assigned transfer ID stable through every transition and continue using it
as the journal entry ID when posting succeeds.

Use these initial states and transitions:

```text
pending_risk -> approved -> completed
            |            \-> failed
            |-> review_required
            \-> declined

review_required -> approved | declined  (future manual-review slice)
```

`completed`, `failed`, and `declined` are terminal. `failed` represents a durable
business failure discovered while attempting to post, such as insufficient funds or
an account that is no longer eligible. Infrastructure failures do not change business
state; the Kafka delivery remains uncommitted and is retried.

Add a positive aggregate version and increment it once per committed transition.
Every transition checks the expected state and version in PostgreSQL. A stale or
duplicate command becomes a no-op only when its previously committed result matches;
otherwise it fails closed as a state conflict.

The schema migration will replace the current mandatory foreign key from transfer ID
to journal entry with a nullable `journal_entry_id`. Existing rows are backfilled as
`completed` at aggregate version 1 with `journal_entry_id = id`. A completed transfer
must have `journal_entry_id = id`; every non-completed transfer must have no journal
entry. Exact migration SQL, indexes, and constraints require a separate database
review before implementation.

### Request acceptance and idempotency

Accept a valid command by storing a `pending_risk` transfer and a
`transfer.requested` outbox event in one serializable transaction. Preserve the
requester-scoped idempotency contract from ADR-0006. An identical replay returns the
current transfer state, while reuse of the key for different intent remains a
conflict. Unlike the synchronous flow, deterministic declines and posting failures
are durable and are returned by later replays.

At acceptance, verify identity, ownership, account type, status, and currency. Do not
debit, reserve, or hold funds. The posting worker rechecks every financial precondition
inside the final serializable ledger transaction because balances and account status
may change after acceptance.

Capture a non-empty `risk_policy_version` when the request is accepted and include it
in `transfer.requested`. Workers keep an immutable registry of supported policy
versions until no event can reference them. An unknown version is an operational
configuration error: do not commit the consumer inbox entry or Kafka offset.

### Deterministic risk engine

Implement risk evaluation as a pure domain operation:

```text
Evaluation = Evaluate(Policy, Input)
```

The input contains only the immutable, pseudonymous transfer facts needed by the
selected policy. A validated policy uses integer arithmetic and explicit
currency-specific thresholds; it does not read the clock, environment, database,
Redis, network, or AI. Redis velocity signals are added in the next slice through an
explicit captured input and degraded-mode policy, not as hidden engine dependencies.

The pure evaluation contains:

- the policy version;
- an integer score from 0 through 1000;
- one of `approve`, `review`, or `decline`;
- a deterministically ordered set of stable signal codes and integer contributions.

Persist assessments as append-only records with the pure evaluation, the exact policy
version and captured input required to reproduce it, and an assessment time supplied
by the application boundary. Do not overwrite an earlier assessment. Policy fixtures
and table-driven tests are part of the compatibility contract. Any rule or threshold
change creates a new policy version.

The risk engine returns a decision but does not load or mutate transfers. The
`risk-worker` application coordinates envelope decoding, policy lookup, assessment,
and persistence through narrow consumer-owned interfaces.

### Events and transaction boundaries

Use the existing `wallet.events.v1` topic and `transfer:<transfer_id>` record key. Add
version 1 contracts for:

- `transfer.requested`, containing canonical intent and `risk_policy_version`;
- `transfer.risk_assessed`, containing policy version, score, decision, and signals;
- `transfer.failed`, containing a stable business reason code and no free-form data.

Continue using `transfer.completed` version 1 for its existing payload. Its aggregate
version becomes the version produced by the completed transition. Historical
completed transfers remain valid at aggregate version 1; a new asynchronous transfer
normally completes at a later version. Consumers must not assume that event type
implies one fixed aggregate version.

For each consumed event, reserve `(consumer_name, event_id)`, apply the state change,
append any assessment, and write the resulting outbox event in one PostgreSQL
transaction. Commit the Kafka offset only after that transaction commits. The risk
worker maps `approve`, `review`, and `decline` to `approved`, `review_required`, and
`declined`. The transaction worker acts only on `approved` transfers and atomically
posts the ledger entry, moves the transfer to `completed`, and writes
`transfer.completed`.

If final financial validation fails, the transaction worker atomically moves the
transfer to `failed` and writes `transfer.failed`; it never writes a partial ledger
effect. Unknown event or payload versions, corrupt stored data, and state conflicts
fail closed. Dead-letter and operator replay policy remain required before claiming
production readiness.

### Delivery sequence

Implement this decision in reviewable increments:

1. pure lifecycle and risk domain types with deterministic policy tests;
2. reviewed migration plus PostgreSQL transition and assessment repositories;
3. `transfer.requested` and risk-result event contracts;
4. risk-worker consumer with inbox/outbox atomicity and redelivery tests;
5. transaction-worker posting path with concurrency and failure-recovery tests;
6. API command/query surface and a visible end-to-end demo.

Do not enable asynchronous API behavior until the migration, both workers, and the
recovery tests are complete.

## Consequences

Accepted requests become observable before money moves, and every deterministic
decision is replayable and auditable. At-least-once delivery does not duplicate a
transition, assessment, ledger posting, or emitted event. Versioned policies prevent
deployment timing from changing the decision for an already accepted request.

The transfer model and schema become more complex. Approval is explicitly not a funds
reservation, so an approved transfer may later fail a financial precondition. Workers
must retain old policy versions while referenced events exist, and operations still
need a dead-letter and replay procedure.

Existing completed rows and events remain valid. Consumers must handle aggregate
versions instead of assuming every `transfer.completed` event has aggregate version 1.

## Rejected alternatives

- Keep synchronous posting and assess afterward: risk could explain a transfer but
  could not prevent a deterministic high-risk posting.
- Create a separate `transfer_requests` aggregate: it avoids migrating the table but
  duplicates identity and idempotency ownership and creates a fragile handoff to the
  completed transfer record.
- Reserve funds while risk runs: long-lived reservations add expiry, release, and
  reconciliation semantics beyond this slice and can strand customer funds.
- Read the current risk policy only when a worker receives an event: a retry after a
  deployment could produce a different result for the same accepted request.
- Put Redis or AI calls inside the deterministic engine: availability and probabilism
  would leak into the financial decision boundary.
- Use Kafka transactions as the source of truth: Kafka cannot atomically commit the
  PostgreSQL ledger and does not replace inbox/outbox idempotency.

## Revisit when

Revisit fund reservation only with explicit expiry and reconciliation requirements.
Revisit reassessment rules before allowing policy changes or velocity updates to
re-evaluate a pending transfer. Revisit state retention and policy-registry cleanup
after measuring traffic. Resolve dead-letter, operator replay, and manual-review
authorization before production consumers or human decisions are enabled.
