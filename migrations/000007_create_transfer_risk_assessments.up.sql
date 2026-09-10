CREATE TABLE transfer_risk_assessments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transfer_id UUID NOT NULL,
    lifecycle_version BIGINT NOT NULL,
    causation_event_id UUID NOT NULL,
    policy_version TEXT COLLATE "C" NOT NULL,
    captured_input JSONB NOT NULL,
    score SMALLINT NOT NULL,
    decision TEXT COLLATE "C" NOT NULL,
    signals JSONB NOT NULL,
    assessed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT transfer_risk_assessments_transfer_lifecycle_key
        UNIQUE (transfer_id, lifecycle_version),
    CONSTRAINT transfer_risk_assessments_causation_event_key
        UNIQUE (causation_event_id),
    CONSTRAINT transfer_risk_assessments_transfer_fkey
        FOREIGN KEY (transfer_id)
        REFERENCES transfers (id)
        ON DELETE RESTRICT,
    CONSTRAINT transfer_risk_assessments_lifecycle_version_check
        CHECK (lifecycle_version >= 2),
    CONSTRAINT transfer_risk_assessments_policy_version_check
        CHECK (
            octet_length(policy_version) BETWEEN 1 AND 64
            AND policy_version ~ '^[!-~]+$' COLLATE "C"
        ),
    CONSTRAINT transfer_risk_assessments_input_check
        CHECK (
            jsonb_typeof(captured_input) = 'object'
            AND octet_length(captured_input::text) <= 65536
        ),
    CONSTRAINT transfer_risk_assessments_score_check
        CHECK (score BETWEEN 0 AND 1000),
    CONSTRAINT transfer_risk_assessments_decision_check
        CHECK (decision IN ('approve', 'review', 'decline')),
    CONSTRAINT transfer_risk_assessments_signals_check
        CHECK (
            jsonb_typeof(signals) = 'array'
            AND octet_length(signals::text) <= 65536
        )
);

CREATE INDEX transfer_risk_assessments_transfer_assessed_at_idx
    ON transfer_risk_assessments (transfer_id, assessed_at, id);

COMMENT ON TABLE transfer_risk_assessments IS
    'Append-only deterministic risk assessments captured before a transfer lifecycle transition.';

COMMENT ON COLUMN transfer_risk_assessments.captured_input IS
    'Exact pseudonymous input used to reproduce the deterministic policy evaluation.';
