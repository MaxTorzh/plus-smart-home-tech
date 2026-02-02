CREATE SCHEMA IF NOT EXISTS delivery;

CREATE TABLE IF NOT EXISTS delivery.deliveries
(
    delivery_id UUID PRIMARY KEY,
    order_id UUID NOT NUll,
    delivery_state VARCHAR(50) NOT NUll CHECK (
    delivery_state IN ( 'CREATED', 'IN_PROGRESS', 'DELIVERED', 'FAILED', 'CANCELED')),

    from_country VARCHAR(100),
    from_city VARCHAR(100),
    from_street VARCHAR(100),
    from_house VARCHAR(20),
    from_flat VARCHAR(20),

    to_country VARCHAR(100),
    to_city VARCHAR(100),
    to_street VARCHAR(100),
    to_house VARCHAR(20),
    to_flat VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_order_id ON delivery.deliveries(order_id);