"""
COMP 8157 Group 7 - Cassandra isolated baseline benchmark
(built on top of Alyan Khowaja's schema.cql)

Runs OLTP, OLAP, and graph-equivalent queries ONE AT A TIME (single
stream) to get a clean baseline. Same query bodies as
cassandra_worker.py, so the delta between the two files is purely
"isolated" vs "co-scheduled".
"""

import csv
import json
import os
import random
import time

import numpy as np
from cassandra.cluster import Cluster

CASSANDRA_HOSTS = ["127.0.0.1"]
KEYSPACE = "olist_benchmark"
STATES = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "DF", "GO", "PE"]
STATUSES = ["processing", "shipped", "delivered", "invoiced", "canceled"]
REPS = int(os.environ.get("REPS", "20"))
WARMUPS = 2


def timed(fn, reps=REPS, warmups=WARMUPS):
    for _ in range(warmups):
        fn()
    times = []
    for _ in range(reps):
        start = time.time()
        fn()
        times.append((time.time() - start) * 1000)
    return times


def summarize(times):
    arr = np.array(times)
    return {
        "median_ms": round(float(np.percentile(arr, 50)), 4),
        "p99_ms": round(float(np.percentile(arr, 99)), 4),
        "count": len(arr),
    }


def main():
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

    results = {"system": "cassandra"}

    def oltp_op():
        state = random.choice(STATES)
        row = session.execute(select_stmt, (state,)).one()
        if row is not None:
            session.execute(update_stmt, (
                random.choice(STATUSES), state, row.order_purchase_timestamp, row.order_id
            ))

    results["oltp"] = summarize(timed(oltp_op))
    print(f"OLTP -> {results['oltp']}")

    def olap_op():
        list(session.execute(olap_stmt, (random.choice(STATES),)))

    results["olap"] = summarize(timed(olap_op))
    print(f"OLAP -> {results['olap']}")

    def graph_op():
        list(session.execute(graph_stmt, (random.choice(STATES),)))

    results["graph"] = summarize(timed(graph_op))
    print(f"Graph -> {results['graph']}")

    out_json = os.environ.get("RESULTS_OUT", "cassandra_baseline_results.json")
    with open(out_json, "w") as f:
        json.dump(results, f, indent=2)

    out_csv = out_json.replace(".json", ".csv")
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["system", "workload", "median_ms", "p99_ms", "count"])
        for wl in ("oltp", "olap", "graph"):
            m = results[wl]
            w.writerow(["cassandra", wl, m["median_ms"], m["p99_ms"], m["count"]])

    print(f"\nBaseline saved to {out_json} and {out_csv}")
    cluster.shutdown()


if __name__ == "__main__":
    main()
