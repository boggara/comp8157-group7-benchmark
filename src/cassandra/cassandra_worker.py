"""
COMP 8157 Group 7 - Cassandra co-scheduled workload worker
(built on top of Alyan Khowaja's schema.cql / oltp_benchmark.py)

Runs OLTP + OLAP + graph-equivalent streams SIMULTANEOUSLY against
Cassandra for a fixed duration. Exposes run_workload(thread_count,
duration_seconds) with the same signature and return shape as
mongodb_worker.py / pg_worker.py, so the integration harness can
drive all four systems identically.

Graph-equivalent here is the "top recommended products for a customer's
state" wide-row scan, since Cassandra can't do multi-hop joins and this
is the closest thing to a relationship query its partition-key model
supports.
"""

import random
import threading
import time

import numpy as np
from cassandra.cluster import Cluster

CASSANDRA_HOSTS = ["127.0.0.1"]
KEYSPACE = "olist_benchmark"
STATES = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "DF", "GO", "PE"]
STATUSES = ["processing", "shipped", "delivered", "invoiced", "canceled"]


def run_workload(thread_count, duration_seconds=30):
    """Runs all 3 workload types simultaneously for a given duration.
    Called by the group integration harness."""
    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect(KEYSPACE)

    select_stmt = session.prepare(
        "SELECT order_id, order_purchase_timestamp FROM orders_by_state "
        "WHERE customer_state = ? LIMIT 1"
    )
    update_stmt = session.prepare(
        "UPDATE orders_by_state SET order_status = ? "
        "WHERE customer_state = ? AND order_purchase_timestamp = ? AND order_id = ?"
    )
    olap_stmt = session.prepare(
        "SELECT freight_value FROM freight_by_month WHERE customer_state = ? LIMIT 200"
    )
    graph_stmt = session.prepare(
        "SELECT order_id, product_category FROM orders_by_state "
        "WHERE customer_state = ? LIMIT 50"
    )

    results = {"oltp": [], "olap": [], "graph": []}
    stop_event = threading.Event()
    lock = threading.Lock()

    def oltp_worker():
        while not stop_event.is_set():
            state = random.choice(STATES)
            row = session.execute(select_stmt, (state,)).one()
            if row is None:
                continue
            start = time.time()
            session.execute(update_stmt, (
                random.choice(STATUSES), state, row.order_purchase_timestamp, row.order_id
            ))
            end = time.time()
            with lock:
                results["oltp"].append((end - start) * 1000)

    def olap_worker():
        while not stop_event.is_set():
            state = random.choice(STATES)
            start = time.time()
            list(session.execute(olap_stmt, (state,)))
            end = time.time()
            with lock:
                results["olap"].append((end - start) * 1000)

    def graph_worker():
        while not stop_event.is_set():
            state = random.choice(STATES)
            start = time.time()
            list(session.execute(graph_stmt, (state,)))
            end = time.time()
            with lock:
                results["graph"].append((end - start) * 1000)

    threads = [threading.Thread(target=oltp_worker) for _ in range(thread_count)]
    threads.append(threading.Thread(target=olap_worker))
    threads.append(threading.Thread(target=graph_worker))

    for t in threads:
        t.start()
    time.sleep(duration_seconds)
    stop_event.set()
    for t in threads:
        t.join()

    cluster.shutdown()

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
    print("Running Cassandra co-scheduled workload (30 seconds per level)...")
    for t in [1, 10, 50, 100]:
        print(f"\nThread count: {t}")
        r = run_workload(thread_count=t, duration_seconds=30)
        for k in ("oltp", "olap", "graph"):
            m = r[k]
            print(f"  {k.upper():5s} -> p50={m['p50']}ms p95={m['p95']}ms p99={m['p99']}ms ops={m['count']}")
    print("\nDone. The integration harness can import run_workload() from this script.")
