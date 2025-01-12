-- Deploy aac-importer:companies_cleanup to pg
-- requires: companies_enrichment

BEGIN;

ALTER TABLE companies
    DROP COLUMN estimated_revenue_lower,
    DROP COLUMN estimated_revenue_upper,
    DROP COLUMN size_range,
    DROP COLUMN employees_count,
    DROP COLUMN industry,
    DROP COLUMN categories;

COMMIT; 