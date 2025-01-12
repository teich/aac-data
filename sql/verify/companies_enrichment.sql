-- Verify aac-importer:companies_enrichment on pg

BEGIN;

SELECT enrichment_data, enrichment_source, enriched_date
FROM companies
WHERE FALSE;

ROLLBACK; 