-- Verify aac-importer:orders_invoice on pg

BEGIN;

SELECT invoice_number
FROM orders
WHERE FALSE;

ROLLBACK; 