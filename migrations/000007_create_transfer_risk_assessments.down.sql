DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM transfer_risk_assessments) THEN
        RAISE EXCEPTION
            'cannot roll back transfer risk assessments migration while assessment data exists';
    END IF;
END;
$$;

DROP TABLE transfer_risk_assessments;
