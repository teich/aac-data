-- Deploy aac-importer:people_enrichment to pg
-- requires: people

BEGIN;

ALTER TABLE people
    ADD COLUMN enrichment_data JSONB,
    ADD COLUMN enrichment_source TEXT,
    ADD COLUMN enriched_date TIMESTAMP WITH TIME ZONE;

COMMIT;
