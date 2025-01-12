-- Revert aac-importer:line_items from pg

BEGIN;

DROP TABLE line_items;

COMMIT;
