-- Verify aac-importer:companies on pg

BEGIN;

SELECT id, name, domain, estimated_revenue_lower, estimated_revenue_upper, linkedin_url, size_range, 
       employees_count, industry, categories, created_at, updated_at
FROM companies
WHERE FALSE;

ROLLBACK;
