CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    event_type TEXT COLLATE "C" NOT NULL,
    event_version SMALLINT NOT NULL,
    aggregate_type TEXT COLLATE "C" NOT NULL,
    aggregate_id UUID NOT NULL,
    aggregate_version BIGINT NOT NULL,
    correlation_id UUID NOT NULL,
    causation_id UUID,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    claimed_by UUID,
    claimed_until TIMESTAMPTZ,
    last_error TEXT,
    published_at TIMESTAMPTZ,
    CONSTRAINT outbox_events_aggregate_event_key UNIQUE (
        aggregate_type,
        aggregate_id,
        aggregate_version,
        event_type,
        event_version
    ),
    CONSTRAINT outbox_events_event_type_check CHECK (
        octet_length(event_type) BETWEEN 1 AND 128
        AND event_type ~ '^[a-z][a-z0-9]*([.][a-z][a-z0-9]*)*$' COLLATE "C"
    ),
    CONSTRAINT outbox_events_event_version_check CHECK (event_version > 0),
    CONSTRAINT outbox_events_aggregate_type_check CHECK (
        octet_length(aggregate_type) BETWEEN 1 AND 64
        AND aggregate_type ~ '^[a-z][a-z0-9_-]*$' COLLATE "C"
    ),
    CONSTRAINT outbox_events_aggregate_version_check CHECK (aggregate_version > 0),
    CONSTRAINT outbox_events_payload_check CHECK (
        jsonb_typeof(payload) = 'object'
        AND octet_length(payload::text) <= 65536
    ),
    CONSTRAINT outbox_events_attempts_check CHECK (publish_attempts >= 0),
    CONSTRAINT outbox_events_claim_shape_check CHECK (
        (claimed_by IS NULL) = (claimed_until IS NULL)
    ),
    CONSTRAINT outbox_events_published_claim_check CHECK (
        published_at IS NULL OR (claimed_by IS NULL AND claimed_until IS NULL)
    ),
    CONSTRAINT outbox_events_last_error_check CHECK (
        last_error IS NULL OR octet_length(last_error) BETWEEN 1 AND 1024
    ),
    CONSTRAINT outbox_events_availability_check CHECK (available_at >= created_at),
    CONSTRAINT outbox_events_publication_time_check CHECK (
        published_at IS NULL OR published_at >= created_at
    )
);

CREATE INDEX outbox_events_pending_idx
    ON outbox_events (available_at, sequence)
    INCLUDE (claimed_until, publish_attempts)
    WHERE published_at IS NULL;

CREATE TABLE consumer_inbox (
    consumer_name TEXT COLLATE "C" NOT NULL,
    event_id UUID NOT NULL,
    event_type TEXT COLLATE "C" NOT NULL,
    event_version SMALLINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT consumer_inbox_pkey PRIMARY KEY (consumer_name, event_id),
    CONSTRAINT consumer_inbox_name_check CHECK (
        octet_length(consumer_name) BETWEEN 1 AND 128
        AND consumer_name ~ '^[a-z0-9][a-z0-9._-]*$' COLLATE "C"
    ),
    CONSTRAINT consumer_inbox_event_type_check CHECK (
        octet_length(event_type) BETWEEN 1 AND 128
        AND event_type ~ '^[a-z][a-z0-9]*([.][a-z][a-z0-9]*)*$' COLLATE "C"
    ),
    CONSTRAINT consumer_inbox_event_version_check CHECK (event_version > 0)
);

COMMENT ON TABLE outbox_events IS
    'Transactional event intent with recoverable at-least-once publication state.';

COMMENT ON TABLE consumer_inbox IS
    'Consumer-scoped event deduplication committed with PostgreSQL side effects.';
