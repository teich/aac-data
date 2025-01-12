-- Revert aac-importer:orders_invoice from pg

BEGIN;

ALTER TABLE orders DROP COLUMN invoice_number;

COMMIT; 