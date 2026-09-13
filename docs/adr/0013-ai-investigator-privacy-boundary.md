# ADR-0013: AI Investigator privacy boundary

- Status: Accepted
- Date: 2026-09-13

## Decision

The investigator receives only a review-case pseudonym, assessment version,
currency, minor-unit amount, deterministic score, and deterministic signal codes.
It has a consumer-owned, read-only provider port and a deterministic mock provider.
Its output is advisory text only and is not persisted or consumed by money-path
commands.

## Consequences

Provider unavailability is returned to the caller without changing a transfer, case,
ledger, or authorization decision. Requester identities, account IDs, credentials,
free-form customer data, and mutable lifecycle objects never enter the provider
context.

## Revisit triggers

Revisit before adding a network provider, durable investigation records, or analyst
UI/API integration.
