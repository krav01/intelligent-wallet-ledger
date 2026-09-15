CREATE TABLE consumer_quarantine (
    consumer_name TEXT COLLATE "C" NOT NULL,
    topic TEXT COLLATE "C" NOT NULL,
    partition INTEGER NOT NULL,
    record_offset BIGINT NOT NULL,
    reason_code TEXT COLLATE "C" NOT NULL,
    value_excerpt BYTEA NOT NULL,
    value_sha256 BYTEA NOT NULL,
    value_size BIGINT NOT NULL,
    value_truncated BOOLEAN NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT consumer_quarantine_pkey PRIMARY KEY (
        consumer_name,
        topic,
        partition,
        record_offset
    ),
    CONSTRAINT consumer_quarantine_consumer_name_check CHECK (
        octet_length(consumer_name) BETWEEN 1 AND 128
        AND consumer_name ~ '^[a-z0-9][a-z0-9._-]*$' COLLATE "C"
    ),
    CONSTRAINT consumer_quarantine_topic_check CHECK (
        octet_length(topic) BETWEEN 1 AND 249
    ),
    CONSTRAINT consumer_quarantine_partition_check CHECK (partition >= 0),
    CONSTRAINT consumer_quarantine_offset_check CHECK (record_offset >= 0),
    CONSTRAINT consumer_quarantine_reason_check CHECK (
        reason_code IN ('invalid_envelope', 'unsupported_event_type')
    ),
    CONSTRAINT consumer_quarantine_value_excerpt_check CHECK (
        octet_length(value_excerpt) <= 65536
    ),
    CONSTRAINT consumer_quarantine_value_sha256_check CHECK (
        octet_length(value_sha256) = 32
    ),
    CONSTRAINT consumer_quarantine_value_size_check CHECK (value_size >= 0),
    CONSTRAINT consumer_quarantine_value_shape_check CHECK (
        value_truncated = (value_size > octet_length(value_excerpt))
    )
);

CREATE INDEX consumer_quarantine_recent_idx
    ON consumer_quarantine (quarantined_at DESC);

COMMENT ON TABLE consumer_quarantine IS
    'Idempotent capture of Kafka records that a consumer deliberately skips after durable quarantine.';

COMMENT ON COLUMN consumer_quarantine.value_excerpt IS
    'First 65536 bytes of the record value; use topic, partition, and record_offset to recover a larger source record before Kafka retention expires.';
