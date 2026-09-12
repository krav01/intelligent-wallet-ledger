# ADR-0009: Captured velocity input and fail-safe risk review

- Status: Accepted
- Date: 2026-09-12

## Context

Risk assessment is deterministic and replayable. Redis can provide an ephemeral velocity observation, but an outage must not permit an unobserved approval.

## Decision

The application captures a non-negative transfer count and whether velocity was degraded, then passes both values to the pure input. A policy may require review at its threshold. A degraded observation produces `velocity_unavailable` and `review`, unless the configured amount threshold already requires `decline`.

The domain package does not import Redis. This ADR does not enable a Redis client or manual approval endpoint.

## Consequences

Redis failures cannot approve a transfer or mutate the ledger. A later adapter must define counter keys, TTL, atomic operations, and observability before enabling it.

## Rejected alternatives

- Treat an unavailable value as zero: this silently approves during an outage.
- Query Redis inside the policy: this destroys replayability and dependency direction.
- Fail Kafka delivery indefinitely: this turns an outage into consumer lag.

## Revisit when

Revisit when the Redis adapter adds retention semantics, manual-review authorization exists, or review volume requires tuning.
