-- Revert aac-importer:companies_cleanup from pg

BEGIN;

ALTER TABLE companies
    ADD COLUMN estimated_revenue_lower NUMERIC,
    ADD COLUMN estimated_revenue_upper NUMERIC,
    ADD COLUMN size_range TEXT,
    ADD COLUMN employees_count INTEGER,
    ADD COLUMN industry TEXT,
    ADD COLUMN categories TEXT[];

COMMIT; 