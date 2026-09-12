CREATE TABLE transfer_review_cases (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transfer_id UUID NOT NULL,
    assessment_lifecycle_version BIGINT NOT NULL,
    status TEXT COLLATE "C" NOT NULL DEFAULT 'open',
    opened_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT transfer_review_cases_transfer_key UNIQUE (transfer_id),
    CONSTRAINT transfer_review_cases_transfer_fkey
        FOREIGN KEY (transfer_id)
        REFERENCES transfers (id)
        ON DELETE RESTRICT,
    CONSTRAINT transfer_review_cases_assessment_fkey
        FOREIGN KEY (transfer_id, assessment_lifecycle_version)
        REFERENCES transfer_risk_assessments (transfer_id, lifecycle_version)
        ON DELETE RESTRICT,
    CONSTRAINT transfer_review_cases_assessment_version_check
        CHECK (assessment_lifecycle_version >= 2),
    CONSTRAINT transfer_review_cases_status_check
        CHECK (status = 'open')
);

COMMENT ON TABLE transfer_review_cases IS
    'Open durable manual-review work created atomically with a review-required risk assessment.';

COMMENT ON COLUMN transfer_review_cases.assessment_lifecycle_version IS
    'Exact review-required assessment version that opened this case.';
