DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM transfers
        WHERE status <> 'completed'
            OR state_version <> 1
            OR risk_policy_version IS NOT NULL
            OR journal_entry_id IS DISTINCT FROM id
            OR failure_reason IS NOT NULL
    ) THEN
        RAISE EXCEPTION
            'cannot roll back transfer lifecycle migration while asynchronous transfers exist';
    END IF;
END;
$$;

DROP INDEX transfers_status_requested_at_idx;

ALTER TABLE transfers
    DROP CONSTRAINT transfers_journal_entry_fkey,
    DROP CONSTRAINT transfers_lifecycle_shape_check,
    DROP CONSTRAINT transfers_failure_reason_check,
    DROP CONSTRAINT transfers_risk_policy_version_check,
    DROP CONSTRAINT transfers_state_version_check,
    DROP CONSTRAINT transfers_status_check,
    DROP COLUMN failure_reason,
    DROP COLUMN journal_entry_id,
    DROP COLUMN risk_policy_version,
    DROP COLUMN state_version,
    DROP COLUMN status,
    ADD CONSTRAINT transfers_journal_entry_fkey
        FOREIGN KEY (id)
        REFERENCES journal_entries (id)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED;
