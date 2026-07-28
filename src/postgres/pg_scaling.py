"""
COMP 8157 Group 7 - PostgreSQL index scan efficiency at scale
Sai Srinivas Uppara - PostgreSQL pipeline

Measures how point-lookup and aggregation latency change as the order
set scales 10K -> 50K -> 107K, mirroring the MongoDB scaling.py so the
two curves are directly comparable.

For each scale a temp working set (orders_scale_temp + its items) is
built, indexed, and VACUUM ANALYZE'd so the planner has fresh
statistics; every timed measurement is preceded by warm-up runs and
the median is reported, so numbers reflect index behavior rather than
cold-cache noise.
"""

import json
import os
import statistics
import time

import psycopg2

PG_DSN = os.environ.get(
    "PG_DSN", "host=localhost port=5432 dbname=olist user=bench password=benchpass123"
)
LOOKUP_REPS = int(os.environ.get("LOOKUP_REPS", "20"))
AGG_REPS = int(os.environ.get("AGG_REPS", "3"))
WARMUPS = 2


def timed_median(cur, sql, params=(), reps=3, warmups=WARMUPS):
    for _ in range(warmups):
        cur.execute(sql, params)
        cur.fetchall()
    times = []
    for _ in range(reps):
        start = time.time()
        cur.execute(sql, params)
        cur.fetchall()
        times.append((time.time() - start) * 1000)
    return round(statistics.median(times), 4)


def main():
    conn = psycopg2.connect(PG_DSN)
    conn.set_client_encoding("UTF8")
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT count(*) FROM orders")
    total = cur.fetchone()[0]
    print(f"Total orders: {total}")
    subsets = {"10K": min(10000, total), "50K": min(50000, total), "107K": total}

    results = {}
    for label, size in subsets.items():
        # Build the working set for this scale.
        cur.execute("DROP TABLE IF EXISTS orders_scale_temp")
        cur.execute("DROP TABLE IF EXISTS items_scale_temp")
        cur.execute(f"""
            CREATE TABLE orders_scale_temp AS
            SELECT * FROM orders ORDER BY order_id LIMIT {size}
        """)
        cur.execute("""
            CREATE TABLE items_scale_temp AS
            SELECT oi.* FROM order_items oi
            JOIN orders_scale_temp o ON o.order_id = oi.order_id
        """)
        cur.execute("CREATE UNIQUE INDEX ON orders_scale_temp(order_id)")
        cur.execute("CREATE INDEX ON orders_scale_temp(customer_id)")
        cur.execute("CREATE INDEX ON items_scale_temp(order_id)")
        cur.execute("VACUUM ANALYZE orders_scale_temp")
        cur.execute("VACUUM ANALYZE items_scale_temp")

        # Point lookup on the indexed key (median of LOOKUP_REPS, warm cache).
        cur.execute(f"SELECT order_id FROM orders_scale_temp OFFSET {size // 2} LIMIT 1")
        test_id = cur.fetchone()[0]
        lookup_ms = timed_median(
            cur,
            "SELECT * FROM orders_scale_temp WHERE order_id = %s",
            (test_id,),
            reps=LOOKUP_REPS,
        )

        # Aggregation: top customers by spend (same logical query as Mongo's).
        agg_ms = timed_median(
            cur,
            """
            SELECT o.customer_id, SUM(i.price) AS total
            FROM orders_scale_temp o
            JOIN items_scale_temp i ON i.order_id = o.order_id
            GROUP BY o.customer_id
            ORDER BY total DESC
            LIMIT 10
            """,
            reps=AGG_REPS,
        )

        results[label] = {"point_lookup_ms": lookup_ms, "aggregation_ms": agg_ms}
        print(f"{label}: point_lookup={lookup_ms}ms | aggregation={agg_ms}ms")

    cur.execute("DROP TABLE IF EXISTS orders_scale_temp")
    cur.execute("DROP TABLE IF EXISTS items_scale_temp")

    out = os.environ.get("RESULTS_OUT", "pg_scaling_results.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out}")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
