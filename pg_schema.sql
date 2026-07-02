-- ============================================================
-- COMP 8157 Group 7 - PostgreSQL relational schema (Olist)
-- Sai Srinivas Uppara - PostgreSQL pipeline
--
-- Nine Olist tables with primary keys and foreign keys enforced
-- so the relational model is a faithful baseline, not a flat dump.
-- Load order (parents before children):
--   customers, sellers, products, category_translation, geolocation
--   -> orders -> order_items, order_payments, order_reviews
-- ============================================================

DROP TABLE IF EXISTS order_reviews  CASCADE;
DROP TABLE IF EXISTS order_payments CASCADE;
DROP TABLE IF EXISTS order_items    CASCADE;
DROP TABLE IF EXISTS orders         CASCADE;
DROP TABLE IF EXISTS geolocation    CASCADE;
DROP TABLE IF EXISTS category_translation CASCADE;
DROP TABLE IF EXISTS products       CASCADE;
DROP TABLE IF EXISTS sellers        CASCADE;
DROP TABLE IF EXISTS customers      CASCADE;

-- ---------- parent tables ----------

CREATE TABLE customers (
    customer_id              CHAR(32) PRIMARY KEY,
    customer_unique_id       CHAR(32) NOT NULL,
    customer_zip_code_prefix VARCHAR(5),
    customer_city            TEXT,
    customer_state           CHAR(2)
);

CREATE TABLE sellers (
    seller_id              CHAR(32) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(5),
    seller_city            TEXT,
    seller_state           CHAR(2)
);

CREATE TABLE products (
    product_id                 CHAR(32) PRIMARY KEY,
    product_category_name      TEXT,             -- NULL for some products in the raw data
    product_name_lenght        NUMERIC,          -- column names keep Olist's original spelling
    product_description_lenght NUMERIC,
    product_photos_qty         NUMERIC,
    product_weight_g           NUMERIC,
    product_length_cm          NUMERIC,
    product_height_cm          NUMERIC,
    product_width_cm           NUMERIC
);

CREATE TABLE category_translation (
    product_category_name         TEXT PRIMARY KEY,
    product_category_name_english TEXT
);

-- Geolocation has no natural key in the raw data (many rows per zip prefix),
-- so it gets a surrogate key and no FK from customers/sellers.
CREATE TABLE geolocation (
    geolocation_row_id          BIGSERIAL PRIMARY KEY,
    geolocation_zip_code_prefix VARCHAR(5),
    geolocation_lat             DOUBLE PRECISION,
    geolocation_lng             DOUBLE PRECISION,
    geolocation_city            TEXT,
    geolocation_state           CHAR(2)
);

-- ---------- child tables ----------

CREATE TABLE orders (
    order_id                      CHAR(32) PRIMARY KEY,
    customer_id                   CHAR(32) NOT NULL REFERENCES customers(customer_id),
    order_status                  VARCHAR(20),
    order_purchase_timestamp      TIMESTAMP,
    order_approved_at             TIMESTAMP,
    order_delivered_carrier_date  TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,      -- NULL for undelivered orders
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE order_items (
    order_id            CHAR(32) NOT NULL REFERENCES orders(order_id),
    order_item_id       SMALLINT NOT NULL,
    product_id          CHAR(32) NOT NULL REFERENCES products(product_id),
    seller_id           CHAR(32) NOT NULL REFERENCES sellers(seller_id),
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(10,2),
    freight_value       NUMERIC(10,2),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE order_payments (
    order_id             CHAR(32) NOT NULL REFERENCES orders(order_id),
    payment_sequential   SMALLINT NOT NULL,
    payment_type         VARCHAR(20),
    payment_installments SMALLINT,
    payment_value        NUMERIC(10,2),
    PRIMARY KEY (order_id, payment_sequential)
);

-- review_id is NOT unique in the raw Olist CSV (a review can span orders),
-- so the primary key is the (review_id, order_id) pair.
CREATE TABLE order_reviews (
    review_id               CHAR(32) NOT NULL,
    order_id                CHAR(32) NOT NULL REFERENCES orders(order_id),
    review_score            SMALLINT,
    review_comment_title    TEXT,
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    PRIMARY KEY (review_id, order_id)
);

-- ============================================================
-- Indexes on the join/filter columns used by the OLTP + OLAP
-- workloads (PKs above already index the lookup keys).
-- ============================================================

CREATE INDEX idx_orders_customer          ON orders(customer_id);
CREATE INDEX idx_orders_status            ON orders(order_status);
CREATE INDEX idx_orders_purchase_ts       ON orders(order_purchase_timestamp);
CREATE INDEX idx_items_product            ON order_items(product_id);
CREATE INDEX idx_items_seller             ON order_items(seller_id);
CREATE INDEX idx_products_category        ON products(product_category_name);
CREATE INDEX idx_reviews_order            ON order_reviews(order_id);
CREATE INDEX idx_geo_zip                  ON geolocation(geolocation_zip_code_prefix);
CREATE INDEX idx_customers_state          ON customers(customer_state);
CREATE INDEX idx_sellers_state            ON sellers(seller_state);
