"""
COMP 8157 Group 7 - PostgreSQL co-scheduled workload worker
Sai Srinivas Uppara - PostgreSQL pipeline

Runs OLTP + OLAP + graph-equivalent streams SIMULTANEOUSLY against
PostgreSQL for a fixed duration. Exposes run_workload(thread_count,
duration_seconds) with the exact same signature and return shape as
mongodb_worker.py, so the group integration harness can drive all four
systems identically. The delta between these numbers and pg_baseline.py
is the interference delta.
"""

import os
import threading
import time

import numpy as np
import psycopg2
import psycopg2.pool

PG_DSN = os.environ.get(
    "PG_DSN", "host=localhost port=5432 dbname=olist user=bench password=benchpass123"
)

OLAP_SQL = """
    SELECT o.customer_id, SUM(oi.price) AS total
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    GROUP BY o.customer_id
    ORDER BY total DESC
    LIMIT 10
"""

GRAPH_SQL = """
    WITH purchases AS (
        SELECT o.customer_id, oi.product_id
        FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
    )
    SELECT rec.product_id, COUNT(*) AS strength
    FROM purchases target
    JOIN purchases other ON other.product_id = target.product_id
                        AND other.customer_id <> target.customer_id
    JOIN purchases rec   ON rec.customer_id = other.customer_id
    WHERE target.customer_id = %s
    GROUP BY rec.product_id
    ORDER BY strength DESC
    LIMIT 10
"""


def run_workload(thread_count, duration_seconds=30):
    """Runs all 3 workload types simultaneously for a given duration.
    Called by the group integration harness."""
    pool = psycopg2.pool.ThreadedConnectionPool(1, thread_count + 5, dsn=PG_DSN)
    results = {"oltp": [], "olap": [], "graph": []}
    stop_event = threading.Event()
    lock = threading.Lock()

    # sample ids
    conn = pool.getconn()
    with conn.cursor() as cur:
        cur.execute("SELECT order_id FROM orders LIMIT 1")
        order_id = cur.fetchone()[0]
        cur.execute("""SELECT o.customer_id FROM orders o
                       JOIN order_items oi ON oi.order_id = o.order_id LIMIT 1""")
        customer_id = cur.fetchone()[0]
    pool.putconn(conn)

    def stream_worker(key, fn):
        conn = pool.getconn()
        conn.set_client_encoding("UTF8")
        try:
            while not stop_event.is_set():
                start = time.time()
                fn(conn)
                end = time.time()
                with lock:
                    results[key].append((end - start) * 1000)
        finally:
            pool.putconn(conn)

    def oltp_op(conn):
        with conn.cursor() as cur:
            cur.execute("UPDATE orders SET order_status='delivered' WHERE order_id=%s", (order_id,))
        conn.commit()

    def olap_op(conn):
        with conn.cursor() as cur:
            cur.execute(OLAP_SQL)
            cur.fetchall()
        conn.commit()

    def graph_op(conn):
        with conn.cursor() as cur:
            cur.execute(GRAPH_SQL, (customer_id,))
            cur.fetchall()
        conn.commit()

    threads = [threading.Thread(target=stream_worker, args=("oltp", oltp_op))
               for _ in range(thread_count)]
    threads.append(threading.Thread(target=stream_worker, args=("olap", olap_op)))
    threads.append(threading.Thread(target=stream_worker, args=("graph", graph_op)))

    for t in threads:
        t.start()
    time.sleep(duration_seconds)
    stop_event.set()
    for t in threads:
        t.join()
    pool.closeall()

    def metrics(data):
        if not data:
            return {"p50": 0, "p95": 0, "p99": 0, "count": 0}
        return {
            "p50": round(float(np.percentile(data, 50)), 4),
            "p95": round(float(np.percentile(data, 95)), 4),
            "p99": round(float(np.percentile(data, 99)), 4),
            "count": len(data),
        }

    return {
        "threads": thread_count,
        "duration_seconds": duration_seconds,
        "oltp": metrics(results["oltp"]),
        "olap": metrics(results["olap"]),
        "graph": metrics(results["graph"]),
    }


if __name__ == "__main__":
    duration = int(os.environ.get("DURATION", "30"))
    print(f"Running PostgreSQL co-scheduled workload ({duration} seconds per level)...")
    for t in [1, 10, 50, 100]:
        print(f"\nThread count: {t}")
        r = run_workload(thread_count=t, duration_seconds=duration)
        for k in ("oltp", "olap", "graph"):
            m = r[k]
            print(f"  {k.upper():5s} -> p50={m['p50']}ms p95={m['p95']}ms p99={m['p99']}ms ops={m['count']}")
    print("\nDone. The integration harness can import run_workload() from this script.")
