CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.products
(
    product_id UUID PRIMARY KEY,
    fragile    BOOLEAN NOT NULL,
    width      DECIMAL(10, 2) NOT NULL,
    height     DECIMAL(10, 2) NOT NULL,
    depth      DECIMAL(10, 2) NOT NULL,
    weight     DECIMAL(10, 2) NOT NULL,
    quantity   BIGINT NOT NULL CHECK (quantity >= 0)
);