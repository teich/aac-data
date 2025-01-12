-- Verify aac-importer:products on pg

BEGIN;

SELECT id, name, description, sku, created_at, updated_at
FROM products
WHERE FALSE;

ROLLBACK;
