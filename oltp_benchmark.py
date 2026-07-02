"""
oltp_benchmark.py
Owner: Alyan Khowaja (Cassandra role, Group 7)

OLTP insert + update workload for Cassandra, simulating order placement
and order-status updates. Runs at concurrency levels 1 / 10 / 50 / 100
using Python's threading module, capturing per-operation latency at the
driver level and reporting p50 / p95 / p99 plus throughput (ops/sec).

Usage:
    python oltp_benchmark.py --workload insert --ops 2000
    python oltp_benchmark.py --workload update --ops 2000
    python oltp_benchmark.py --workload insert --ops 2000 --out results/oltp_insert.csv
"""

import argparse
import random
import string
import threading
import time
import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from cassandra.cluster import Cluster

CASSANDRA_HOSTS = ["127.0.0.1"]
KEYSPACE = "olist_benchmark"
CONCURRENCY_LEVELS = [1, 10, 50, 100]

STATES = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "DF", "GO", "PE"]
STATUSES = ["processing", "shipped", "delivered", "invoiced", "canceled"]


def _random_order_id():
    return "bench-" + "".join(random.choices(string.hexdigits, k=16)).lower()


class LatencyRecorder:
    """Thread-safe latency collector, one instance per benchmark run."""

    def __init__(self):
        self._lock = threading.Lock()
        self._latencies_ms = []

    def record(self, latency_ms: float):
        with self._lock:
            self._latencies_ms.append(latency_ms)

    def summary(self, elapsed_s: float) -> dict:
        arr = np.array(self._latencies_ms)
        return {
            "count": len(arr),
            "throughput_ops_sec": round(len(arr) / elapsed_s, 2) if elapsed_s > 0 else 0,
            "p50_ms": round(float(np.percentile(arr, 50)), 3),
            "p95_ms": round(float(np.percentile(arr, 95)), 3),
            "p99_ms": round(float(np.percentile(arr, 99)), 3),
            "mean_ms": round(float(arr.mean()), 3),
        }


def insert_op(session, insert_stmt, recorder: LatencyRecorder):
    state = random.choice(STATES)
    ts = datetime.utcnow() - timedelta(days=random.randint(0, 700))
    order_id = _random_order_id()
    customer_id = str(uuid.uuid4())

    start = time.perf_counter()
    session.execute(insert_stmt, (
        state, ts, order_id, customer_id, "bench_city", "processing",
        ts, None, ts + timedelta(days=7),
        round(random.uniform(20, 900), 2), round(random.uniform(5, 80), 2),
        "bench_category", random.choice(STATES),
    ))
    latency_ms = (time.perf_counter() - start) * 1000
    recorder.record(latency_ms)


def update_op(session, select_stmt, update_stmt, recorder: LatencyRecorder):
    state = random.choice(STATES)
    # pick a real existing row to update, staying within Cassandra's
    # "query by partition key" constraint
    row = session.execute(select_stmt, (state,)).one()
    if row is None:
        return
    new_status = random.choice(STATUSES)

    start = time.perf_counter()
    session.execute(update_stmt, (new_status, state, row.order_purchase_timestamp, row.order_id))
    latency_ms = (time.perf_counter() - start) * 1000
    recorder.record(latency_ms)


def run_at_concurrency(session, workload: str, stmts: dict, n_threads: int, total_ops: int) -> dict:
    recorder = LatencyRecorder()
    ops_per_thread = max(1, total_ops // n_threads)

    def worker():
        for _ in range(ops_per_thread):
            if workload == "insert":
                insert_op(session, stmts["insert"], recorder)
            else:
                update_op(session, stmts["select"], stmts["update"], recorder)

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start

    result = recorder.summary(elapsed)
    result["concurrency"] = n_threads
    result["elapsed_s"] = round(elapsed, 2)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workload", choices=["insert", "update"], required=True)
    parser.add_argument("--ops", type=int, default=2000, help="Total ops per concurrency level")
    parser.add_argument("--hosts", nargs="+", default=CASSANDRA_HOSTS)
    parser.add_argument("--out", default=None, help="Optional CSV output path")
    args = parser.parse_args()

    cluster = Cluster(args.hosts)
    session = cluster.connect(KEYSPACE)

    insert_stmt = session.prepare("""
        INSERT INTO orders_by_state (
            customer_state, order_purchase_timestamp, order_id, customer_id,
            customer_city, order_status, order_approved_at,
            order_delivered_customer_date, order_estimated_delivery_date,
            payment_value, freight_value, product_category, seller_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    select_stmt = session.prepare("""
        SELECT order_id, order_purchase_timestamp FROM orders_by_state
        WHERE customer_state = ? LIMIT 1
    """)
    update_stmt = session.prepare("""
        UPDATE orders_by_state SET order_status = ?
        WHERE customer_state = ? AND order_purchase_timestamp = ? AND order_id = ?
    """)
    stmts = {"insert": insert_stmt, "select": select_stmt, "update": update_stmt}

    all_results = []
    for n_threads in CONCURRENCY_LEVELS:
        print(f"Running {args.workload} workload at concurrency={n_threads} ...")
        result = run_at_concurrency(session, args.workload, stmts, n_threads, args.ops)
        result["workload"] = args.workload
        all_results.append(result)
        print(f"  {result}")

    df = pd.DataFrame(all_results)
    print("\n=== Summary ===")
    print(df.to_string(index=False))

    if args.out:
        df.to_csv(args.out, index=False)
        print(f"\nResults written to {args.out}")

    cluster.shutdown()


if __name__ == "__main__":
    main()
