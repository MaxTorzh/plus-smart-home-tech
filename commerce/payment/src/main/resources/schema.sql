CREATE SCHEMA IF NOT EXISTS payment;

CREATE TABLE IF NOT EXISTS payment.payments
(
    payment_id UUID PRIMARY KEY,
    order_id UUID NOT NUll,
    delivery_total NUMERIC(19,2),
    total_payment NUMERIC(19,2),
    fee_total NUMERIC(19,2),
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING' CHECK
    (status IN ('PENDING', 'SUCCESS', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payment.payments(order_id);