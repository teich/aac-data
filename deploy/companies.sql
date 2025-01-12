-- Deploy aac-importer:companies to pg

BEGIN;

CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    domain TEXT UNIQUE,
    linkedin_url TEXT,
    enrichment_data JSONB,
    enrichment_source TEXT,
    enriched_date TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add an index on domain as it might be used for lookups
CREATE INDEX idx_companies_domain ON companies(domain);

COMMIT;
