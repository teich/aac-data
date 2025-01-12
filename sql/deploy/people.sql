-- Deploy aac-importer:people to pg
-- requires: companies

BEGIN;

CREATE TABLE people (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    phone TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    country TEXT,
    company_id INTEGER NOT NULL REFERENCES companies(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add index on company_id for foreign key lookups
CREATE INDEX idx_people_company_id ON people(company_id);
-- Add unique index on email as it's typically unique per person
CREATE UNIQUE INDEX idx_people_email ON people(email);

COMMIT;
