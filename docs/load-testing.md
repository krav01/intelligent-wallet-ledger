# Load testing

`scripts/k6/healthz.js` exercises the real unauthenticated `GET /healthz` contract
with 20 virtual users for 15 seconds. It requires every request and check to succeed
and rejects a p95 HTTP duration at or above 250 ms. The scenario is deliberately an
availability smoke workload, not a financial-workflow benchmark: the repository has
no unauthenticated transfer-creation endpoint and the analyst command requires an
operator-provided OIDC identity and durable review-case data.

Run it against a locally started API with a locally installed k6 binary:

```bash
make run
K6_BASE_URL=http://127.0.0.1:8080 make load-health
```

GitHub Actions runs the same scenario against the built `wallet-api` binary and keeps
the k6 result summary in the job log. That result is reproducible functional evidence
for the stated runner, not a production capacity claim: it has no external database,
Kafka, Redis, OIDC, TLS proxy, network policy, or production traffic mix.

Before using load data for a capacity decision, run an environment-specific workload
against representative dependencies and authenticated review traffic, record the
image digest, machine class, topology, duration, SLO, and summary, then compare
repeated samples. Do not infer money-path throughput from this health endpoint.
