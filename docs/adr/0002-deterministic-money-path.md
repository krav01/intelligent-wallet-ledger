# ADR-0002: Deterministic money path and advisory AI

- Status: Accepted
- Date: 2026-09-09

## Context

LLM output is probabilistic, may be unavailable, and may expose unnecessary personal
data. Financial posting requires repeatable, auditable decisions and strict invariants.

## Decision

Only deterministic domain code and PostgreSQL transactions may authorize, decline,
post, or reverse financial operations. The AI Investigator receives minimized,
pseudonymous context for an existing review case and returns a human-readable advisory
explanation. No money-path command accepts AI output as authority.

## Consequences

AI outages cannot stop transfers or corrupt balances. The investigator can improve
operator understanding without becoming a financial source of truth. A human remains
responsible for manual-review actions.

## Revisit when

Do not weaken this boundary. A future model may enrich deterministic signals, but any
new influence on financial decisions requires a security review, measurable evidence,
and a superseding ADR.
