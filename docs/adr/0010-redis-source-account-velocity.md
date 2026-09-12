# ADR-0010: Redis source-account velocity observations

- Status: Accepted
- Date: 2026-09-13

## Context

Policies can require a captured transfer-count observation, but the deterministic risk
domain must neither query Redis nor depend on live process state. Kafka delivers at
least once, so retries must not inflate a velocity window. The captured observation
must also remain available with the assessment for audit and replay.

## Decision

When an immutable policy has a positive velocity review threshold, the risk worker
uses Redis to observe transfers by source account. The key is
`risk:velocity:source-account:<source_account_id>`; a sorted-set member is the stable
transfer ID. One Lua command removes members outside the configured sliding window,
adds the current transfer with `ZADD NX`, applies a TTL, and returns `ZCARD`. The
current transfer is included in the returned count.

The worker reserves the inbox event and validates the locked pending lifecycle before
calling the observer. A redelivery whose inbox reservation already exists returns
without a second Redis command. Redis commands have one-second dial, read, and write
timeouts and no client retry. A Redis error is logged and becomes
`VelocityDegraded: true`; the existing deterministic policy then produces an auditable
review decision. The worker passes the exact resulting `riskdomain.Input` to both the
policy and the append-only assessment record.

The adapter is enabled only when a policy has a velocity threshold. It requires
`REDIS_ADDRESS` and `RISK_VELOCITY_WINDOW`; otherwise startup configuration fails.

## Consequences

The pure domain remains replayable and has no Redis dependency. Duplicate Kafka
delivery cannot inflate the counter after a successful database commit. A crash after
the Redis command but before the PostgreSQL commit can leave one idempotent member;
retrying in the same window reuses it, while a later retry makes a new observation at
its actual assessment time.

The worker holds its PostgreSQL transaction while executing one bounded Redis command.
This favors duplicate safety and captured-input consistency over avoiding a short
external call while a pending transfer is locked.

No manual-review case record, analyst decision, or RBAC API is introduced here. Those
need a durable actor and authorization contract before they can change a transfer from
`review_required`.

## Rejected alternatives

- Increment Redis before deduplicating the inbox event: Kafka redelivery can inflate
  the count.
- Query Redis in the deterministic policy: this breaks replayability and dependency
  direction.
- Retry Kafka indefinitely on a Redis outage: an infrastructure outage becomes
  consumer lag instead of an explicit review decision.

## Revisit when

Revisit when Redis command latency affects PostgreSQL lock contention, when source
account is not the correct abuse boundary, or before adding manual case ownership and
analyst authorization.
