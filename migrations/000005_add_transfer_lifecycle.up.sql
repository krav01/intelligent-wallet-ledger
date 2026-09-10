ALTER TABLE transfers
    DROP CONSTRAINT transfers_journal_entry_fkey,
    ADD COLUMN status TEXT COLLATE "C" NOT NULL DEFAULT 'completed',
    ADD COLUMN state_version BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN risk_policy_version TEXT COLLATE "C",
    ADD COLUMN journal_entry_id UUID,
    ADD COLUMN failure_reason TEXT COLLATE "C";

UPDATE transfers
SET journal_entry_id = id;

ALTER TABLE transfers
    ADD CONSTRAINT transfers_status_check
        CHECK (
            status IN (
                'pending_risk',
                'approved',
                'review_required',
                'declined',
                'completed',
                'failed'
            )
        ),
    ADD CONSTRAINT transfers_state_version_check
        CHECK (state_version > 0),
    ADD CONSTRAINT transfers_risk_policy_version_check
        CHECK (
            risk_policy_version IS NULL
            OR (
                octet_length(risk_policy_version) BETWEEN 1 AND 64
                AND risk_policy_version ~ '^[!-~]+$' COLLATE "C"
            )
        ),
    ADD CONSTRAINT transfers_failure_reason_check
        CHECK (
            failure_reason IS NULL
            OR (
                octet_length(failure_reason) BETWEEN 1 AND 64
                AND failure_reason ~ '^[a-z0-9_-]+$' COLLATE "C"
            )
        ),
    ADD CONSTRAINT transfers_lifecycle_shape_check
        CHECK (
            (
                status = 'pending_risk'
                AND state_version = 1
                AND risk_policy_version IS NOT NULL
                AND journal_entry_id IS NULL
                AND failure_reason IS NULL
            )
            OR (
                status IN ('approved', 'review_required', 'declined')
                AND state_version >= 2
                AND risk_policy_version IS NOT NULL
                AND journal_entry_id IS NULL
                AND failure_reason IS NULL
            )
            OR (
                status = 'completed'
                AND journal_entry_id IS NOT NULL
                AND journal_entry_id = id
                AND failure_reason IS NULL
                AND (state_version = 1 OR risk_policy_version IS NOT NULL)
            )
            OR (
                status = 'failed'
                AND state_version >= 2
                AND risk_policy_version IS NOT NULL
                AND journal_entry_id IS NULL
                AND failure_reason IS NOT NULL
            )
        ),
    ADD CONSTRAINT transfers_journal_entry_fkey
        FOREIGN KEY (journal_entry_id)
        REFERENCES journal_entries (id)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX transfers_status_requested_at_idx
    ON transfers (status, requested_at, id);

COMMENT ON COLUMN transfers.status IS
    'Durable transfer lifecycle state; only completed rows reference a journal entry.';

COMMENT ON COLUMN transfers.state_version IS
    'Positive aggregate version incremented once for every committed lifecycle transition.';

COMMENT ON COLUMN transfers.risk_policy_version IS
    'Immutable deterministic risk policy version captured when an asynchronous transfer is accepted.';
