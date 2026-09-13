CREATE TABLE transfer_review_audit_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transfer_id UUID NOT NULL,
    lifecycle_version BIGINT NOT NULL,
    decision TEXT COLLATE "C" NOT NULL,
    actor_subject TEXT COLLATE "C" NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT transfer_review_audit_records_transfer_fkey
        FOREIGN KEY (transfer_id)
        REFERENCES transfers (id)
        ON DELETE RESTRICT,
    CONSTRAINT transfer_review_audit_records_transfer_version_key
        UNIQUE (transfer_id, lifecycle_version),
    CONSTRAINT transfer_review_audit_records_version_check
        CHECK (lifecycle_version >= 3),
    CONSTRAINT transfer_review_audit_records_decision_check
        CHECK (decision IN ('approved', 'declined')),
    CONSTRAINT transfer_review_audit_records_actor_subject_check
        CHECK (
            octet_length(actor_subject) BETWEEN 1 AND 128
            AND actor_subject ~ '^[!-~]+$' COLLATE "C"
        )
);

COMMENT ON TABLE transfer_review_audit_records IS
    'Append-only audit records created atomically when an analyst resolves a transfer review case.';

COMMENT ON COLUMN transfer_review_audit_records.lifecycle_version IS
    'Exact transfer lifecycle version produced by the recorded review decision.';
