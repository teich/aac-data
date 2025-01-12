-- Verify aac-importer:companies_cleanup on pg

BEGIN;

-- Try to select the dropped columns - this should fail if they were properly dropped
DO $$
BEGIN
    PERFORM estimated_revenue_lower FROM companies WHERE FALSE;
    RAISE EXCEPTION 'estimated_revenue_lower column still exists';
EXCEPTION WHEN undefined_column THEN
    -- This is what we want
END;
END $$;

ROLLBACK; 