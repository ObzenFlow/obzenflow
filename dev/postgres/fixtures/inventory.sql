-- Schema used by the independent PostgreSQL inventory consumer test.
-- `schema` is a psql identifier variable supplied by repository tooling.
CREATE SCHEMA IF NOT EXISTS :"schema";

CREATE TABLE IF NOT EXISTS :"schema".inventory_levels (
    warehouse TEXT NOT NULL,
    sku TEXT NOT NULL,
    available_units BIGINT NOT NULL,
    replenishment_due BOOLEAN NOT NULL,
    PRIMARY KEY (warehouse, sku)
);
