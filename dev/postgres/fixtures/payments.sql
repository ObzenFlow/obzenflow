-- Schema used by the PostgreSQL payments learning example.
-- `schema` is a psql identifier variable supplied by repository tooling.
CREATE SCHEMA IF NOT EXISTS :"schema";

CREATE TABLE IF NOT EXISTS :"schema".payments (
    payment_id BIGINT PRIMARY KEY,
    order_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    amount_cents BIGINT NOT NULL CHECK (amount_cents >= 0)
);
