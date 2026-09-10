ALTER TABLE outbox_events
    DROP CONSTRAINT outbox_events_event_type_check,
    ADD CONSTRAINT outbox_events_event_type_check CHECK (
        octet_length(event_type) BETWEEN 1 AND 128
        AND event_type ~ '^[a-z][a-z0-9_]*([.][a-z][a-z0-9_]*)*$' COLLATE "C"
    );

ALTER TABLE consumer_inbox
    DROP CONSTRAINT consumer_inbox_event_type_check,
    ADD CONSTRAINT consumer_inbox_event_type_check CHECK (
        octet_length(event_type) BETWEEN 1 AND 128
        AND event_type ~ '^[a-z][a-z0-9_]*([.][a-z][a-z0-9_]*)*$' COLLATE "C"
    );
