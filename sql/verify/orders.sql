-- Verify aac-importer:orders on pg

BEGIN;

SELECT id, person_id, date, amount, created_at, updated_at
FROM orders
WHERE FALSE;

ROLLBACK;
