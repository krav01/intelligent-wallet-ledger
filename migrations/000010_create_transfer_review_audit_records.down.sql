DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM transfer_review_audit_records) THEN
        RAISE EXCEPTION
            'cannot roll back transfer review audit records migration while audit data exists';
    END IF;
END;
$$;

DROP TABLE transfer_review_audit_records;
