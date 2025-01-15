-- Revert aac-importer:qb_sales_orders from pg

BEGIN;

-- Drop the index first
DROP INDEX IF EXISTS idx_orders_order_number;

-- Remove the columns
ALTER TABLE orders DROP COLUMN IF EXISTS source;
ALTER TABLE orders DROP COLUMN IF EXISTS channel;
ALTER TABLE orders DROP COLUMN IF EXISTS order_number;

COMMIT;
