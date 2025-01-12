-- Revert aac-importer:people from pg

BEGIN;

DROP TABLE people;

COMMIT;
