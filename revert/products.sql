-- Revert aac-importer:products from pg

BEGIN;

DROP TABLE products;

COMMIT;
