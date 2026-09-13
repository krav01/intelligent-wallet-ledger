# Observability

`wallet-api` exposes Prometheus-compatible metrics on `GET /metrics`. The endpoint
uses a per-process registry and publishes HTTP traffic and latency under
`wallet_http_requests_total` and `wallet_http_request_duration_seconds`.

Both metrics use only bounded labels:

- `method` is the HTTP method;
- `route` is the registered route pattern, or `unmatched` for a 404;
- `status` is the HTTP response status.

Raw paths, transfer IDs, OIDC subjects, request bodies, and headers are never metric
labels. A basic latency query is:

```promql
histogram_quantile(
  0.99,
  sum by (le, route) (rate(wallet_http_request_duration_seconds_bucket[5m]))
)
```

The metrics endpoint is intentionally not a Prometheus server, Grafana deployment, or
distributed tracing setup. Those are separate roadmap slices.
