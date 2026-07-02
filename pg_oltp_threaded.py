"""
COMP 8157 Group 7 - PostgreSQL OLTP harness
Sai Srinivas Uppara - PostgreSQL pipeline

Parameterized INSERT / UPDATE / point-lookup workload simulating order
placement and inventory changes, run at 1 / 10 / 50 / 100 concurrent
threads. Per-operation latency is captured at the driver level and
reported as p50 / p95 / p99 in ms — same output format as the MongoDB
oltp_threaded.py so results merge cleanly.

Uses a psycopg2 ThreadedConnectionPool (the default connection limit is
exhausted at 100 threads otherwise) and retry-with-backoff on deadlock
for update-heavy runs.
"""

import json
import os
import random
import threading
import time
import uuid

import numpy as np
import psycopg2
import psycopg2.errors
import psycopg2.pool

PG_DSN = os.environ.get(
    "PG_DSN", "host=localhost port=5432 dbname=olist user=bench password=benchpass123"
)
OPS_PER_THREAD = int(os.environ.get("OPS_PER_THREAD", "10"))
THREAD_LEVELS = [1, 10, 50, 100]
MAX_RETRIES = 5

pool = psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=110, dsn=PG_DSN)


def with_retry(fn):
    """Run fn(); on deadlock/serialization failure, retry with backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            return fn()
        except (psycopg2.errors.DeadlockDetected, psycopg2.errors.SerializationFailure):
            time.sleep(0.01 * (2 ** attempt) * random.random())
    return fn()  # final attempt, let it raise


def run_oltp(thread_count, operation, label):
    latencies = []
    lock = threading.Lock()

    def worker():
        conn = pool.getconn()
        conn.set_client_encoding("UTF8")
        try:
            for _ in range(OPS_PER_THREAD):
                start = time.time()
                with_retry(lambda: operation(conn))
                end = time.time()
                with lock:
                    latencies.append((end - start) * 1000)
        finally:
            pool.putconn(conn)

    threads = [threading.Thread(target=worker) for _ in range(thread_count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    p50 = round(float(np.percentile(latencies, 50)), 4)
    p95 = round(float(np.percentile(latencies, 95)), 4)
    p99 = round(float(np.percentile(latencies, 99)), 4)
    throughput = round(len(latencies) / (sum(latencies) / 1000 / thread_count), 2)
    print(f"  {label} | threads={thread_count} | p50={p50}ms p95={p95}ms p99={p99}ms | ~{throughput} ops/s")
    return {"threads": thread_count, "p50": p50, "p95": p95, "p99": p99, "ops_per_sec": throughput}


# ---- sample IDs for parameterized statements ----
_conn = psycopg2.connect(PG_DSN)
_cur = _conn.cursor()
_cur.execute("SELECT order_id, customer_id FROM orders LIMIT 1")
SAMPLE_ORDER_ID, SAMPLE_CUSTOMER_ID = _cur.fetchone()
_cur.execute("SELECT product_id, seller_id FROM order_items LIMIT 1")
SAMPLE_PRODUCT_ID, SAMPLE_SELLER_ID = _cur.fetchone()
_cur.close()
_conn.close()


# --- INSERT: simulate order placement (new order + one line item), then clean up ---
def insert_op(conn):
    oid = uuid.uuid4().hex  # 32-char id, matches Olist id format
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp)
               VALUES (%s, %s, 'created', now())""",
            (oid, SAMPLE_CUSTOMER_ID),
        )
        cur.execute(
            """INSERT INTO order_items (order_id, order_item_id, product_id, seller_id, price, freight_value)
               VALUES (%s, 1, %s, %s, %s, %s)""",
            (oid, SAMPLE_PRODUCT_ID, SAMPLE_SELLER_ID, round(random.uniform(10, 300), 2), 9.90),
        )
        conn.commit()
        # keep dataset size stable across runs
        cur.execute("DELETE FROM order_items WHERE order_id = %s", (oid,))
        cur.execute("DELETE FROM orders WHERE order_id = %s", (oid,))
        conn.commit()


# --- UPDATE: simulate status/inventory change on an existing order ---
def update_op(conn):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE orders SET order_status = 'delivered' WHERE order_id = %s",
            (SAMPLE_ORDER_ID,),
        )
        conn.commit()


# --- POINT LOOKUP: fetch a single order by primary key ---
def lookup_op(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM orders WHERE order_id = %s", (SAMPLE_ORDER_ID,))
        cur.fetchone()
        conn.commit()


if __name__ == "__main__":
    results = {}

    print("INSERT operation (order placement):")
    results["insert"] = [run_oltp(t, insert_op, "INSERT") for t in THREAD_LEVELS]

    print("UPDATE operation (order status):")
    results["update"] = [run_oltp(t, update_op, "UPDATE") for t in THREAD_LEVELS]

    print("POINT LOOKUP operation:")
    results["lookup"] = [run_oltp(t, lookup_op, "LOOKUP") for t in THREAD_LEVELS]

    out = os.environ.get("RESULTS_OUT", "pg_oltp_threaded_results.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out}")
    pool.closeall()
