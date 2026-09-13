# ADR-0015: Atomic audit records for analyst review decisions

- Status: Accepted
- Date: 2026-09-13

## Context

`transfer_review_cases` preserves the latest decision, while the transactional
outbox represents delivery intent and is later mutated by the publisher. Neither is
a durable audit record owned by the review-decision workflow. A manual approval or
decline needs an attributable, queryable record that cannot be committed separately
from its lifecycle change.

## Decision

Create one append-only `transfer_review_audit_records` row in the existing
serializable review-decision transaction. Each row stores the transfer, resulting
lifecycle version, approved or declined decision, trusted principal subject, and
decision time. A unique transfer/version key prevents a second audit record for the
same decision transition.

Audit insertion occurs after the lifecycle and review-case updates and before the
outbox insert. Any audit failure aborts the shared transaction, so no approved or
declined state can commit without its audit record.

## Consequences

The audit trail is durable and atomic for analyst decisions. It is intentionally
limited: it does not introduce an identity provider, RBAC policy engine, generic
audit-event abstraction, retention policy, or privileged database role. Application
code has no update or delete path for audit records; database permission hardening is
a deployment concern to add with authenticated operational roles.

## Revisit triggers

Revisit when an authenticated API, additional actor-driven commands, retention or
export requirements, or database role separation is introduced.
