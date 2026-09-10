DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM outbox_events
        WHERE position('_' IN event_type) > 0
    ) OR EXISTS (
        SELECT 1
        FROM consumer_inbox
        WHERE position('_' IN event_type) > 0
    ) THEN
        RAISE EXCEPTION
            'cannot roll back event type migration while underscored event types exist';
    END IF;
END;
$$;

ALTER TABLE outbox_events
    DROP CONSTRAINT outbox_events_event_type_check,
    ADD CONSTRAINT outbox_events_event_type_check CHECK (
        octet_length(event_type) BETWEEN 1 AND 128
        AND event_type ~ '^[a-z][a-z0-9]*([.][a-z][a-z0-9]*)*$' COLLATE "C"
    );

ALTER TABLE consumer_inbox
    DROP CONSTRAINT consumer_inbox_event_type_check,
    ADD CONSTRAINT consumer_inbox_event_type_check CHECK (
        octet_length(event_type) BETWEEN 1 AND 128
        AND event_type ~ '^[a-z][a-z0-9]*([.][a-z][a-z0-9]*)*$' COLLATE "C"
    );
