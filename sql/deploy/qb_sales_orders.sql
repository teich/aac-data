-- Deploy aac-importer:qb_sales_orders to pg
-- requires: orders

BEGIN;

-- Add new columns for QB sales support
-- Add columns as nullable first
ALTER TABLE orders ADD COLUMN order_number TEXT;
ALTER TABLE orders ADD COLUMN channel TEXT;
ALTER TABLE orders ADD COLUMN source TEXT;

-- Set default values for existing records
UPDATE orders SET order_number = 'LEGACY-' || id::text;
UPDATE orders SET channel = 'legacy';

-- Now add NOT NULL constraints
ALTER TABLE orders ALTER COLUMN order_number SET NOT NULL;
ALTER TABLE orders ALTER COLUMN channel SET NOT NULL;

-- Add index for order number lookups
CREATE INDEX idx_orders_order_number ON orders(order_number);

COMMIT;
