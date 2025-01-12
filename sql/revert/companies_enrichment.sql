-- Revert aac-importer:companies_enrichment from pg

BEGIN;

ALTER TABLE companies
    DROP COLUMN enrichment_data,
    DROP COLUMN enrichment_source,
    DROP COLUMN enriched_date;

COMMIT; 