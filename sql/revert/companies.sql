-- Revert aac-importer:companies from pg

BEGIN;

DROP TABLE companies;

COMMIT;
