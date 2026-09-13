DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM transfer_review_cases WHERE status <> 'open') THEN
        RAISE EXCEPTION
            'cannot roll back review case decisions migration while decided cases exist';
    END IF;
END;
$$;

ALTER TABLE transfer_review_cases
    DROP CONSTRAINT transfer_review_cases_decision_check,
    DROP CONSTRAINT transfer_review_cases_status_check,
    DROP COLUMN decided_at,
    DROP COLUMN decided_by,
    DROP COLUMN decision,
    ADD CONSTRAINT transfer_review_cases_status_check CHECK (status = 'open');
