DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM transfer_review_cases) THEN
        RAISE EXCEPTION
            'cannot roll back transfer review cases migration while case data exists';
    END IF;
END;
$$;

DROP TABLE transfer_review_cases;
