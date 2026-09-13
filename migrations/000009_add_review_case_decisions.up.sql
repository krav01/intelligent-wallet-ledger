ALTER TABLE transfer_review_cases
    ADD COLUMN decision TEXT COLLATE "C",
    ADD COLUMN decided_by TEXT COLLATE "C",
    ADD COLUMN decided_at TIMESTAMPTZ;

ALTER TABLE transfer_review_cases
    DROP CONSTRAINT transfer_review_cases_status_check,
    ADD CONSTRAINT transfer_review_cases_status_check
        CHECK (status IN ('open', 'approved', 'declined')),
    ADD CONSTRAINT transfer_review_cases_decision_check
        CHECK (
            (status = 'open' AND decision IS NULL AND decided_by IS NULL AND decided_at IS NULL)
            OR
            (status IN ('approved', 'declined')
                AND decision = status
                AND octet_length(decided_by) BETWEEN 1 AND 128
                AND decided_by ~ '^[!-~]+$' COLLATE "C"
                AND decided_at IS NOT NULL)
        );

COMMENT ON COLUMN transfer_review_cases.decided_by IS
    'Subject from the trusted principal supplied by the authenticated adapter.';
