# ADR-0017: Per-principal review-decision rate limiting

- Status: Accepted
- Date: 2026-09-13

## Context

The OIDC review-decision API authenticates and authorizes analysts, but a valid
analyst credential can still repeatedly invoke the command. The review-case
transaction prevents conflicting state changes, yet repeated commands consume
database capacity and create avoidable contention. Client IP headers are not a
trusted identity boundary and must not determine the command budget.

## Decision

After OIDC token verification and the `analyst` role check, the HTTP adapter applies
an in-process token bucket keyed by the verified principal subject. The default is
12 commands per minute with an initial burst of two; deployments may configure 1 to
600 through `REVIEW_DECISION_RATE_LIMIT_PER_MINUTE`. A denied command returns `429`
before its body is decoded or the review-case transaction is called.

The limiter holds at most 10,000 recently seen subjects and removes idle entries
after 15 minutes. It does not read client IP or forwarding headers. Invalid or
unauthorized requests never receive a principal-specific bucket.

## Consequences

The control limits repeated authorized commands without changing the trusted
principal contract, financial transaction, audit record, or outbox semantics. It is
bounded in memory and has deterministic unit coverage for token refill and handler
rejection.

The budget is per wallet-api process, so replicas have independent buckets. It is a
deliberate single-process protection, not a distributed quota. A shared, fail-safe
limiter is required before horizontally scaling analyst command traffic.

## Rejected alternatives

- Keying by IP or forwarded headers would trust client-controlled transport data.
- Limiting before token verification would let attackers consume an analyst budget.
- Persisting every rate-limit update in PostgreSQL would add write load to an
  intentionally lightweight pre-transaction guard.

## Revisit triggers

Revisit before multiple wallet-api replicas, when limits need tenant or case scope,
or when a durable operator quota and retry-after contract are required.
