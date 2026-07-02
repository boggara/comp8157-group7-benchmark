-- =====================================================================
-- schema.cql
-- Cassandra wide-column schema for the Olist e-commerce benchmark
-- Owner: Alyan Khowaja (Cassandra role, Group 7)
--
-- Design notes:
--   * Primary access pattern is "get orders for a customer state within
--     a time window", so customer_state is the partition key and
--     order_purchase_timestamp is the clustering column (DESC so the
--     most recent orders come back first, matching dashboard queries).
--   * customer_state has enough cardinality (27 Brazilian states) to
--     spread writes reasonably, while still being a meaningful
--     analytical grouping -- this is the trade-off documented in the
--     proposal's Risks & Mitigation section (hot-partition risk).
--   * A second table (seller_region_delivery) pre-aggregates delivery
--     data by seller_state so the OLAP "avg delivery time by seller
--     region" query doesn't require a cross-partition join, which
--     Cassandra can't do natively.
--   * A counter table tracks per-customer order counts within a state
--     partition to answer "top customers by order count" without
--     scanning the whole orders table.
-- =====================================================================

CREATE KEYSPACE IF NOT EXISTS olist_benchmark
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE olist_benchmark;

-- ---------------------------------------------------------------------
-- Main wide-row table: orders partitioned by customer_state, clustered
-- by order_purchase_timestamp. This is the table the OLTP insert/update
-- scripts write to and the OLAP revenue/freight queries read from.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders_by_state (
    customer_state                  text,
    order_purchase_timestamp        timestamp,
    order_id                        text,
    customer_id                     text,
    customer_city                   text,
    order_status                    text,
    order_approved_at               timestamp,
    order_delivered_customer_date   timestamp,
    order_estimated_delivery_date   timestamp,
    payment_value                   decimal,
    freight_value                   decimal,
    product_category                text,
    seller_state                    text,
    PRIMARY KEY ((customer_state), order_purchase_timestamp, order_id)
) WITH CLUSTERING ORDER BY (order_purchase_timestamp DESC)
  AND comment = 'Wide-row order table, partitioned by customer_state';

-- ---------------------------------------------------------------------
-- Pre-aggregated delivery table, partitioned by seller_state, so the
-- "average delivery time per seller region" OLAP query stays within a
-- single partition instead of needing ALLOW FILTERING on the full table.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS seller_region_delivery (
    seller_state                text,
    order_purchase_timestamp    timestamp,
    order_id                    text,
    delivery_days                int,
    PRIMARY KEY ((seller_state), order_purchase_timestamp, order_id)
) WITH CLUSTERING ORDER BY (order_purchase_timestamp DESC)
  AND comment = 'Delivery time rollup, partitioned by seller_state';

-- ---------------------------------------------------------------------
-- Counter table for "top customers by order count within a state".
-- CQL can't ORDER BY a counter value, so the query script pulls the
-- partition and sorts client-side (documented as a CQL constraint in
-- the proposal / contribution log).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customer_order_counts (
    customer_state    text,
    customer_id       text,
    order_count       counter,
    PRIMARY KEY ((customer_state), customer_id)
);

-- ---------------------------------------------------------------------
-- Freight cost bucketed by month, partitioned by customer_state, so
-- "freight cost distribution over rolling time windows" doesn't require
-- scanning the full orders_by_state table.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS freight_by_month (
    customer_state    text,
    year_month        text,     -- e.g. '2018-04'
    order_id          text,
    freight_value     decimal,
    PRIMARY KEY ((customer_state), year_month, order_id)
);
