# ADR-0011: Durable transfer review cases

- Status: Accepted
- Date: 2026-09-13

## Context

The risk worker can advance a transfer from `pending_risk` to `review_required`.
That lifecycle status alone is not a durable unit of analyst work: future queueing,
assignment, and investigation need a stable record linked to the assessment that
caused the review. The service has no authenticated analyst identity, role model,
or decision API yet.

## Decision

Create one `transfer_review_cases` row in the same PostgreSQL transaction that
persists a `review_required` lifecycle transition and its risk assessment. The case
stores the transfer ID, the exact assessment lifecycle version, and its opening
time. A composite foreign key links it to the corresponding
`transfer_risk_assessments` row; a unique transfer key allows one case per
transfer under the current lifecycle.

The initial case state is deliberately only `open`. This slice does not create an
outbox event for the case and does not expose case retrieval, assignment, closure,
or analyst approve/decline actions.

## Consequences

Risk assessment and review-case creation commit or roll back together, so a
review-required transfer cannot be left without its durable work item. Future AI
investigation may start from the case after a queueing contract is defined.

Any future analyst action must introduce authenticated identity, role-based
authorization, an explicit case lifecycle, and an audit trail before it can alter
the transfer lifecycle. It must also define how an open case is resolved when a
transfer becomes approved or declined.

## Rejected alternatives

- Treating `review_required` as the work item would lose case-specific lifecycle
  and assignment history.
- Creating cases asynchronously in another consumer could leave a committed
  review-required transfer without a case after delivery failure.
- Adding analyst decisions now would create an authorization boundary without an
  identity or RBAC model.

## Revisit triggers

Revisit this decision when implementing an analyst queue, case assignment or
closure, AI investigator input, or authenticated analyst decisions.
