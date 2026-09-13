# ADR-0019: OpenTelemetry HTTP trace propagation

- Status: Accepted
- Date: 2026-09-14

## Decision

The shared wallet API HTTP server extracts W3C `traceparent` and creates a server span
named `wallet-api.http`. It preserves the incoming trace ID and does not use paths,
transfer IDs, OIDC subjects, headers, or request bodies as span names or attributes.

The process does not configure a `TracerProvider` or exporter. Deployment
infrastructure owns collector endpoint, sampling, credentials, and retention; without
a provider the OpenTelemetry API remains a no-op.

## Consequences

HTTP trace context is ready for a deployment-provided provider without coupling money
logic to observability infrastructure. Exporters, database spans, Kafka propagation,
and log correlation remain separate slices.
