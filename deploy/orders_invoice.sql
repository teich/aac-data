-- Deploy aac-importer:orders_invoice to pg
-- requires: orders

BEGIN;

ALTER TABLE orders ADD COLUMN invoice_number TEXT UNIQUE;
CREATE INDEX idx_orders_invoice_number ON orders(invoice_number);

COMMIT; 