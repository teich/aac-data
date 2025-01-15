-- Revert aac-importer:people_enrichment from pg

BEGIN;

ALTER TABLE people
    DROP COLUMN enrichment_data,
    DROP COLUMN enrichment_source,
    DROP COLUMN enriched_date;

COMMIT;
