# intelligent-wallet-ledger Helm chart

This chart deploys the wallet API, outbox publisher, risk worker, and transaction
worker. PostgreSQL, Kafka, Redis, OIDC, the image registry, TLS termination, and
schema migrations stay operator-owned dependencies.

## Prerequisites

- Kubernetes cluster with a CNI that enforces `NetworkPolicy` if it is enabled.
- An immutable image digest for the repository's multi-binary image.
- Four existing Secrets, one per process. Do not commit their values or database URLs.
- PostgreSQL schema migrations applied before starting workers.
- Reachable PostgreSQL, Kafka, Redis (when velocity is enabled), and OIDC issuer
  endpoints. Configure their TLS, authentication, and network policy outside this
  chart.

Each process Secret must provide only the environment variables that process needs.
At minimum, all workers require `DATABASE_URL` and `KAFKA_BROKERS`; the risk worker
also requires either `RISK_POLICIES_JSON` or the legacy policy variables. The API's
analyst route additionally requires the complete OIDC triplet described in the root
README. Set `KAFKA_ALLOW_AUTO_TOPIC_CREATION=false` in production.

## Render and install

Create the four process-scoped Secrets in the target namespace through the approved
secret manager, then render a release with a digest-pinned image:

```bash
helm lint deploy/helm/intelligent-wallet-ledger \
  --set image.repository=registry.example/intelligent-wallet-ledger \
  --set image.digest=sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

helm upgrade --install wallet deploy/helm/intelligent-wallet-ledger \
  --namespace wallet --create-namespace \
  --set image.repository=registry.example/intelligent-wallet-ledger \
  --set image.digest=sha256:YOUR_RELEASE_DIGEST
```

The chart intentionally has no migration hook: applying an unreviewed schema change
as part of a workload rollout can create an unsafe partial recovery state. Run a
versioned migration job with the release procedure, verify it, and only then install
or upgrade these workloads.

`networkPolicy.enabled=true` restricts only API ingress to port 8080. Egress and
worker traffic remain operator policy because database, broker, Redis, OIDC, DNS, and
metrics endpoints vary by cluster. See the root threat model and runbook before
exposing the API or metrics endpoint.

## Verification boundary

CI runs `helm lint` and `helm template` against representative values. This chart has
not been applied to a Kubernetes cluster from this repository; cluster admission,
network enforcement, secret delivery, migration execution, and runtime behavior need
environment-specific verification.
