# Operational recovery runbook

## Scope and safety boundary

This runbook is for the local Compose demo and for operators adapting the same
transactional-outbox design to an environment they control. It does not authorize
manual balance, posting, lifecycle, inbox, or outbox mutations. The immutable ledger
and PostgreSQL transaction boundaries remain the source of truth.

For production, use environment-specific access controls, encrypted connections,
backups, and an approved incident process. The Compose credentials and plaintext
listeners are development-only.

## First response

1. Record the incident time, affected service, and any transfer or event ID; do not
   paste bearer tokens, connection strings, or customer data into tickets.
2. Check process and dependency health:

   ```bash
   docker compose ps
   docker compose logs --tail=200 wallet-api outbox-publisher risk-worker transaction-worker
   curl -fsS http://localhost:8080/healthz
   curl -fsS http://localhost:8080/readyz
   ```

3. Check the bounded HTTP signals when `wallet-api` is running:

   ```bash
   curl -fsS http://localhost:8080/metrics | rg '^wallet_http_'
   ```

4. Classify the incident before changing anything: dependency outage, publisher delay,
   worker consumer lag, malformed event, or ledger discrepancy.

## Dependency outage

### PostgreSQL unavailable

The API and workers must treat PostgreSQL errors as infrastructure failures. A worker
does not commit its Kafka offset until its inbox, lifecycle, and outbox transaction
succeeds. Restore PostgreSQL connectivity, then restart only the affected process:

```bash
docker compose up -d postgres
docker compose logs --tail=100 postgres
docker compose up -d outbox-publisher risk-worker transaction-worker
```

Do not acknowledge, delete, or recreate `consumer_inbox` rows to force a retry. Kafka
redelivery is safe after the transaction commits because the consumer name and event ID
are reserved atomically.

### Kafka unavailable

Outbox rows stay durable in PostgreSQL until the publisher records broker
acknowledgement. Restore Kafka and restart the publisher; it claims eligible rows using
a lease and republishes after an expired claim when necessary:

```bash
docker compose up -d kafka
docker compose logs --tail=100 kafka outbox-publisher
docker compose up -d outbox-publisher
```

At-least-once delivery is expected. A duplicate broker message is not evidence that a
financial effect was duplicated; the receiving worker's inbox makes a committed repeat
a no-op.

### Redis unavailable

Redis is only used for the risk velocity observation. The risk worker records a
degraded velocity input and routes the assessment to manual review rather than silently
approving a high-risk transfer. Restore Redis and inspect the review-case path; do not
re-evaluate a completed assessment under a new policy version.

```bash
docker compose up -d redis
docker compose logs --tail=100 redis risk-worker
```

## Publisher delay or repeated retries

Use read-only SQL to inspect unpublished work. In the Compose demo, run `psql` in the
database container:

```bash
docker compose exec postgres psql -U wallet -d wallet -c "
SELECT event_id, event_type, aggregate_id, available_at, claimed_until,
       publish_attempts, left(COALESCE(last_error, ''), 160) AS last_error
FROM outbox_events
WHERE published_at IS NULL
ORDER BY available_at, sequence
LIMIT 50;"
```

Interpretation:

- `available_at` in the future means a failed publication is waiting for bounded
  backoff; wait for it or fix the reported dependency.
- A future `claimed_until` means another publisher currently owns the event; do not
  steal the claim.
- An expired `claimed_until` is recoverable: restarting a healthy publisher is
  sufficient. Its claim query accepts expired leases and fencing prevents a stale owner
  from completing the event.
- A rising `publish_attempts` or repeated sanitized `last_error` requires dependency
  diagnosis, not database edits.

Do not set `published_at`, clear claims, or reduce attempts manually. Those fields are
the publication audit trail and are intentionally written only through the publisher
repository.

## Worker consumer lag or malformed event

Inspect the worker and publisher logs first:

```bash
docker compose logs --tail=200 risk-worker transaction-worker outbox-publisher
```

Expected worker behavior:

- A PostgreSQL infrastructure error leaves the Kafka offset uncommitted so the record
  can be retried after recovery.
- A successful database transaction reserves `(consumer_name, event_id)`; a redelivery
  then performs no second durable effect.
- A malformed envelope, malformed payload for an owned event type, or unknown event
  type is stored idempotently in `consumer_quarantine` before its Kafka offset is
  acknowledged. A failed quarantine write leaves the offset uncommitted.
- A known event owned by another worker is acknowledged as ignored. An unavailable
  captured policy remains retryable and is not acknowledged.

For a quarantine alert, identify the row by consumer/topic/partition/offset and record
the reason code plus SHA-256 value fingerprint. Do not log or paste its value excerpt
into an incident. Escalate to the event-contract owner, retrieve the source record by
its stored Kafka position before retention expires when the 64 KiB excerpt is not
complete, then correct and republish it with a new event ID. Do not delete the
quarantine row or modify `consumer_inbox` to force replay.

## Ledger discrepancy

`account_balances` is a transactional read model; journal entries and postings are the
immutable financial source of truth. The repository exposes read-only reconciliation.
Run the existing integration suite to exercise it against a migrated local database:

```bash
TEST_DATABASE_URL='postgres://wallet:wallet_dev_only@localhost:5432/wallet?sslmode=disable' \
  KAFKA_BROKERS='localhost:9092' \
  make test-integration
```

If reconciliation reports drift in an environment with real data:

1. Stop automated remediation and preserve database and application logs.
2. Identify the affected account, currency, journal entries, and snapshot values.
3. Escalate to the financial-data owner; never overwrite balances or postings to make
   the report empty.
4. Correct a confirmed business error with a new, balanced reversal or adjustment
   workflow after review. This repository does not provide an automatic repair command.

## Verification after recovery

Use the smallest relevant check first:

```bash
make test
make test-race
make test-integration
make demo-outbox
make demo-transfer
```

`demo-outbox` visibly proves the acknowledged-but-unrecorded publisher crash path:
the same Kafka event is republished and the consumer's database effect remains exactly
once. `demo-transfer` proves approved and insufficient-funds transaction-worker paths.

## Escalation information

An incident handoff should include:

- time window, deployment revision, and affected process;
- health and dependency status;
- event ID, aggregate/transfer ID, topic/partition/offset when applicable;
- sanitized worker and publisher errors plus outbox `publish_attempts`;
- reconciliation output, if run;
- actions already taken and whether any messages were replayed.

Never include OIDC tokens, database URLs, Kafka credentials, or unredacted request
bodies in the handoff.
