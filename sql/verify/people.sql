-- Verify aac-importer:people on pg

BEGIN;

SELECT id, name, email, phone, address, city, state, zip, country,
       company_id, created_at, updated_at
FROM people
WHERE FALSE;

ROLLBACK;
