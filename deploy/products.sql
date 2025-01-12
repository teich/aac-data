-- Deploy aac-importer:products to pg

BEGIN;

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    sku TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add unique constraint on SKU as it's typically a unique identifier
CREATE UNIQUE INDEX idx_products_sku ON products(sku);

COMMIT;
