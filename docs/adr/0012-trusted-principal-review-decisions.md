# ADR-0012: Trusted principals for review decisions

- Status: Accepted
- Date: 2026-09-13

## Context

Only a human analyst may resolve an open transfer review case. The service has no
HTTP command surface or configured identity provider, so it cannot honestly
authenticate a caller itself. A manual approval must still reach the transaction
worker through the durable outbox; otherwise an approved transfer remains stuck.

## Decision

The review-decision application boundary accepts a `TrustedPrincipal` supplied by a
future authenticated adapter. It accepts only the `analyst` role and a nonempty,
visible-ASCII subject. It never reads an HTTP header or client-supplied role itself.

In one serializable transaction, a valid analyst decision locks the transfer and its
open review case, transitions the lifecycle, records the decision subject and time,
and writes a `transfer.review_decided` outbox event. The transaction worker consumes
that event: an approval proceeds through normal posting; a decline is durably
consumed without posting.

## Consequences

The database stores a minimal decision trail and lifecycle, case, and event cannot
partially commit. This is authorization over a trusted principal contract, not a
complete authentication implementation. A future OIDC, JWT, or mTLS adapter must
verify identity before constructing the principal.

## Rejected alternatives

- Trusting HTTP role headers would allow privilege escalation.
- Approving without an outbox event would strand an approved transfer.
- Adding a concrete identity provider now would select an external trust boundary
  before the API surface exists.

## Revisit triggers

Revisit when an authenticated API, analyst assignment, richer audit history, or
multi-role approval policy is introduced.
