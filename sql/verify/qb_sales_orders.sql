-- Verify aac-importer:qb_sales_orders on pg

BEGIN;

-- Try to select using the new columns - this will fail if they don't exist
SELECT order_number, channel, source
FROM orders
WHERE FALSE;

-- Verify the index exists
SELECT 1/COUNT(*)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = 'idx_orders_order_number'
  AND n.nspname = current_schema();

ROLLBACK;
