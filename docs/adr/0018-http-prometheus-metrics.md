# ADR-0018: Bounded Prometheus HTTP metrics

- Status: Accepted
- Date: 2026-09-14

## Context

The wallet API has structured request logs but no aggregate operational signal for
traffic, failures, or latency. Operators need a scrapeable signal without exposing
transfer IDs, OIDC subjects, request bodies, or arbitrary client paths as high-cardinality
metric labels.

## Decision

Expose `GET /metrics` from the shared HTTP server. Each API process owns its own
Prometheus registry and records `wallet_http_requests_total` and
`wallet_http_request_duration_seconds`. Both use only HTTP method, registered route
pattern (or `unmatched`), and status code labels. The latency metric is a histogram
with Prometheus default buckets so replicas can be aggregated server-side.

The metrics middleware runs after route dispatch so it observes the registered route
pattern and final response status. It does not add raw paths, request bodies, headers,
or authenticated principal data to labels.

## Consequences

The API now exposes bounded, scrapeable request traffic and latency metrics. The
per-process registry avoids global collector conflicts in tests and keeps ownership at
the server boundary. `/metrics` itself is an operational endpoint and is currently
unauthenticated, matching `/healthz` and `/readyz`; deploy it behind network controls
when the runtime environment requires endpoint isolation.

This decision does not introduce OpenTelemetry, an exporter, dashboards, alerts, or
business-level metrics.

## Revisit triggers

Revisit when adding another API process, authentication for operational endpoints,
trace exemplars, business SLOs, or a shared observability deployment.
