-- Revert aac-importer:orders from pg

BEGIN;

DROP TABLE orders;

COMMIT;
