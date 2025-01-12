-- Verify aac-importer:line_items on pg

BEGIN;

SELECT id, order_id, product_id, unit_price, quantity, amount,
       created_at, updated_at
FROM line_items
WHERE FALSE;

ROLLBACK;
