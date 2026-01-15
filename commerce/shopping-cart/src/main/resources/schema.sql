CREATE SCHEMA IF NOT EXISTS cart;

CREATE TABLE IF NOT EXISTS cart.shopping_carts
(
    cart_id UUID PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    cart_state VARCHAR(50) NOT NULL CHECK (cart_state IN ('ACTIVE', 'DEACTIVATED'))
);

CREATE TABLE IF NOT EXISTS cart.shopping_cart_products
(
    cart_id UUID NOT NULL,
    product_id UUID NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (cart_id, product_id),
    FOREIGN KEY (cart_id) REFERENCES cart.shopping_carts(cart_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_cart_username ON cart.shopping_carts(username);