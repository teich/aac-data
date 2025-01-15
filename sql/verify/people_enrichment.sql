-- Verify aac-importer:people_enrichment on pg

BEGIN;

SELECT enrichment_data, enrichment_source, enriched_date
FROM people
WHERE FALSE;

ROLLBACK;
