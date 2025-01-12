-- Deploy aac-importer:companies_enrichment to pg
-- requires: companies

BEGIN;

ALTER TABLE companies
    ADD COLUMN enrichment_data JSONB,
    ADD COLUMN enrichment_source TEXT,
    ADD COLUMN enriched_date TIMESTAMP WITH TIME ZONE;

COMMIT; 