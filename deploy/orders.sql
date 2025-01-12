-- Deploy aac-importer:orders to pg
-- requires: people

BEGIN;

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES people(id),
    date DATE NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add index on person_id for foreign key lookups
CREATE INDEX idx_orders_person_id ON orders(person_id);
-- Add index on date for date-based queries
CREATE INDEX idx_orders_date ON orders(date);

COMMIT;
