DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM consumer_quarantine) THEN
        RAISE EXCEPTION
            'cannot roll back consumer quarantine migration while quarantine records exist';
    END IF;
END;
$$;

DROP TABLE consumer_quarantine;
