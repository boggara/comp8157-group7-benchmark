"""
COMP 8157 Group 7 - PostgreSQL isolated baseline benchmark
Sai Srinivas Uppara - PostgreSQL pipeline

Runs each workload type IN ISOLATION (OLTP, OLAP, recommendation),
three repetitions each with warm-up runs discarded, recording median
and p99 latency plus throughput. These are the baseline numbers that
feed the interference-delta comparison against the co-scheduled runs
(pg_worker.py).

Outputs both JSON and CSV so the metrics owner can merge results
across the four systems.
"""

import csv
import json
import os
import time

import numpy as np
import psycopg2

from pg_olap import QUERIES as OLAP_QUERIES
from pg_recommendation import QUERIES as REC_QUERIES, pick_target_customer

PG_DSN = os.environ.get(
    "PG_DSN", "host=localhost port=5432 dbname=olist user=bench password=benchpass123"
)
REPS = int(os.environ.get("REPS", "3"))
OLTP_OPS = int(os.environ.get("OLTP_OPS", "100"))
WARMUPS = 1


def bench(fn, reps=REPS, warmups=WARMUPS):
    """Run fn() warmups+reps times; return per-rep latencies (ms), warm-ups discarded."""
    for _ in range(warmups):
        fn()
    times = []
    for _ in range(reps):
        start = time.time()
        fn()
        times.append((time.time() - start) * 1000)
    return times


def summarize(times, ops_per_rep=1):
    arr = np.array(times)
    total_s = arr.sum() / 1000
    return {
        "median_ms": round(float(np.percentile(arr, 50)), 4),
        "p99_ms": round(float(np.percentile(arr, 99)), 4),
        "throughput_ops_s": round(float(len(arr) * ops_per_rep / total_s), 2) if total_s > 0 else 0,
    }


def main():
    conn = psycopg2.connect(PG_DSN)
    conn.set_client_encoding("UTF8")
    cur = conn.cursor()
    results = {"system": "postgresql", "reps": REPS}

    # ---------- OLTP baseline (single-threaded, per-op latency) ----------
    cur.execute("SELECT order_id FROM orders LIMIT 1")
    order_id = cur.fetchone()[0]

    def oltp_update():
        cur.execute("UPDATE orders SET order_status='delivered' WHERE order_id=%s", (order_id,))
        conn.commit()

    def oltp_lookup():
        cur.execute("SELECT * FROM orders WHERE order_id=%s", (order_id,))
        cur.fetchone()
        conn.commit()

    per_op = []
    for op in (oltp_update, oltp_lookup):
        op()  # warm-up
        for _ in range(OLTP_OPS):
            start = time.time()
            op()
            per_op.append((time.time() - start) * 1000)
    results["oltp"] = summarize(per_op)
    print(f"OLTP    -> {results['oltp']}")

    # ---------- OLAP baseline (each of the four queries) ----------
    results["olap"] = {}
    for name, sql in OLAP_QUERIES.items():
        def run(sql=sql):
            cur.execute(sql)
            cur.fetchall()
        results["olap"][name] = summarize(bench(run))
        print(f"OLAP    -> {name}: {results['olap'][name]}")

    # ---------- Recommendation baseline (graph-equivalent joins) ----------
    customer_id = pick_target_customer(cur)
    results["recommendation"] = {}
    for name, sql in REC_QUERIES.items():
        def run(sql=sql):
            cur.execute(sql, {"customer_id": customer_id})
            cur.fetchall()
        results["recommendation"][name] = summarize(bench(run))
        print(f"REC     -> {name}: {results['recommendation'][name]}")

    # ---------- write JSON + CSV in the shared metrics format ----------
    out_json = os.environ.get("RESULTS_OUT", "pg_baseline_results.json")
    with open(out_json, "w") as f:
        json.dump(results, f, indent=2)

    out_csv = out_json.replace(".json", ".csv")
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["system", "workload", "query", "median_ms", "p99_ms", "throughput_ops_s"])
        w.writerow(["postgresql", "oltp", "insert_update_lookup",
                    results["oltp"]["median_ms"], results["oltp"]["p99_ms"],
                    results["oltp"]["throughput_ops_s"]])
        for wl in ("olap", "recommendation"):
            for name, m in results[wl].items():
                w.writerow(["postgresql", wl, name, m["median_ms"], m["p99_ms"], m["throughput_ops_s"]])

    print(f"\nBaseline saved to {out_json} and {out_csv}")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
