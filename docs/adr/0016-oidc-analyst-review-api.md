# ADR-0016: OIDC-protected analyst review API

- Status: Accepted
- Date: 2026-09-13

## Context

ADR-0012 deliberately accepted only a `TrustedPrincipal` at the review-decision
application boundary. A transport adapter is now needed to authenticate the analyst
who sends the decision, without making the domain layer parse HTTP headers or select
an identity provider.

## Decision

When all three OIDC settings are configured, `wallet-api` discovers one issuer and
verifies Bearer JWTs against its JWKS using the configured audience. It derives the
principal subject and roles from the verified token. Only a role equal to `analyst`
may call `POST /v1/transfers/{transferID}/review-decision` with one JSON
`decision` field. The handler sends a `TrustedPrincipal` with that subject and role
to the existing review-case application service.

The OIDC issuer, audience, and role-claim name are process configuration, not code.
They must be present together with `DATABASE_URL`; otherwise the analyst route is not
enabled or a partial configuration fails at startup. The HTTP handler limits its body,
rejects unknown fields, and returns generic authentication failures rather than token
verification details.

## Consequences

The command has a concrete, provider-neutral authentication boundary while review
state, audit records, lifecycle transitions, and outbox delivery remain owned by the
existing serializable application transaction. Operational health endpoints continue
to run without an OIDC provider. Deployments enabling analyst commands must provide a
reachable, trustworthy issuer and rotate their issuer keys according to that
provider's policy.

## Rejected alternatives

- Trusting a role header would allow client-side privilege escalation.
- Embedding one vendor's SDK would make the domain deployment-specific.
- Passing raw JWT claims directly to the application service would violate ADR-0012.

## Revisit triggers

Revisit for tenant-aware authorization, multiple identity issuers, fine-grained
approval limits, service-to-service credentials, or a user-facing analyst interface.
