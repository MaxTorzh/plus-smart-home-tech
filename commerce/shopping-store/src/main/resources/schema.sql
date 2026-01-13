CREATE TABLE IF NOT EXISTS products (
  product_id VARCHAR PRIMARY KEY,
  product_name VARCHAR NOT NULL,
  description VARCHAR NOT NULL,
  image_src VARCHAR,
  quantity_state VARCHAR NOT NULL,
  product_state VARCHAR NOT NULL,
  rating INTEGER NOT NULL,
  product_category VARCHAR,
  price double precision
);