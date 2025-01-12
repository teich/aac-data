-- Deploy aac-importer:companies to pg

BEGIN;

CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    domain TEXT UNIQUE,
    estimated_revenue_lower NUMERIC,
    estimated_revenue_upper NUMERIC,
    linkedin_url TEXT,
    size_range TEXT,
    employees_count INTEGER,
    industry TEXT,
    categories TEXT[],
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add an index on domain as it might be used for lookups
CREATE INDEX idx_companies_domain ON companies(domain);

COMMIT;
