CREATE TABLE IF NOT EXISTS warehouse_product (
    product_id VARCHAR PRIMARY KEY,
    quantity   INTEGER,
    fragile    BOOLEAN,
    width      double precision NOT NULL,
    height     double precision NOT NULL,
    depth      double precision NOT NULL,
    weight     double precision NOT NULL
);