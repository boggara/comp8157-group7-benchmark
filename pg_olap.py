"""
COMP 8157 Group 7 - PostgreSQL OLAP queries
Sai Srinivas Uppara - PostgreSQL pipeline

The four analytical queries from the proposal:
  1. Revenue by product category
  2. Average delivery time by seller region (state)
  3. Top customers by lifetime order value
  4. Freight cost distribution over rolling monthly windows

Undelivered orders (NULL delivery dates) are filtered explicitly in
query 2 — otherwise they pull the average and the result is wrong.
Each query is timed at the driver level; results go to JSON.
"""

import json
import os
import time

import psycopg2

PG_DSN = os.environ.get(
    "PG_DSN", "host=localhost port=5432 dbname=olist user=bench password=benchpass123"
)
RUNS = int(os.environ.get("RUNS", "3"))

QUERIES = {
    "revenue_by_category": """
        SELECT COALESCE(p.product_category_name, '(uncategorized)') AS category,
               ROUND(SUM(oi.price), 2) AS total_revenue
        FROM order_items oi
        JOIN products p ON p.product_id = oi.product_id
        GROUP BY 1
        ORDER BY total_revenue DESC
        LIMIT 10
    """,
    "avg_delivery_by_seller_state": """
        SELECT s.seller_state,
               ROUND(AVG(EXTRACT(EPOCH FROM (o.order_delivered_customer_date
                                             - o.order_purchase_timestamp)) / 86400)::numeric, 2)
                   AS avg_delivery_days
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN sellers s ON s.seller_id = oi.seller_id
        WHERE o.order_delivered_customer_date IS NOT NULL   -- exclude undelivered orders
          AND o.order_purchase_timestamp IS NOT NULL
        GROUP BY s.seller_state
        ORDER BY avg_delivery_days ASC
        LIMIT 10
    """,
    "top_customers_by_lifetime_value": """
        SELECT c.customer_unique_id,
               ROUND(SUM(oi.price + oi.freight_value), 2) AS lifetime_value,
               COUNT(DISTINCT o.order_id) AS order_count
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        JOIN order_items oi ON oi.order_id = o.order_id
        GROUP BY c.customer_unique_id
        ORDER BY lifetime_value DESC
        LIMIT 10
    """,
    "freight_distribution_rolling_monthly": """
        SELECT date_trunc('month', o.order_purchase_timestamp) AS month,
               ROUND(AVG(oi.freight_value), 2)  AS avg_freight,
               ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY oi.freight_value)::numeric, 2)
                   AS median_freight,
               ROUND(AVG(AVG(oi.freight_value))
                     OVER (ORDER BY date_trunc('month', o.order_purchase_timestamp)
                           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)::numeric, 2)
                   AS rolling_3mo_avg_freight
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        WHERE o.order_purchase_timestamp IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """,
}


def main():
    conn = psycopg2.connect(PG_DSN)
    conn.set_client_encoding("UTF8")
    cur = conn.cursor()
    results = {}

    for name, sql in QUERIES.items():
        cur.execute(sql)  # warm-up run (not timed) so caches don't skew results
        cur.fetchall()
        times = []
        rows = None
        for _ in range(RUNS):
            start = time.time()
            cur.execute(sql)
            rows = cur.fetchall()
            times.append((time.time() - start) * 1000)
        median_ms = round(sorted(times)[len(times) // 2], 4)
        results[name] = median_ms
        print(f"{name}: {median_ms} ms (median of {RUNS}, {len(rows)} rows)")
        for r in rows[:3]:
            print(f"   {r}")

    out = os.environ.get("RESULTS_OUT", "pg_olap_results.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out}")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
