CREATE TABLE IF NOT EXISTS shopping_cart (
  shopping_cart_id VARCHAR DEFAULT gen_random_uuid() PRIMARY KEY,
  username VARCHAR NOT NULL,
  cart_state boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS shopping_cart_products (
  product_id VARCHAR NOT NULL,
  quantity INTEGER,
  cart_id VARCHAR REFERENCES shopping_cart(shopping_cart_id) ON DELETE CASCADE
);