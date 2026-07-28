"""
COMP 8157 Group 7 - MongoDB isolated baseline benchmark
Mounika Boggarapu - MongoDB pipeline

Runs OLTP, OLAP, and graph($graphLookup) workloads ONE AT A TIME
(single stream, no threads competing) so we get a clean baseline
number for each. This is the "isolated" half of the interference
delta - the "concurrent" half comes from mongodb_worker.run_workload().

Same query bodies as mongodb_worker.py on purpose, so the only thing
that changes between this file and that one is whether OLTP/OLAP/graph
are running alone or all at once.
"""

import csv
import json
import os
import time

import numpy as np
from pymongo import MongoClient

REPS = int(os.environ.get("REPS", "20"))
WARMUPS = 2


def get_db():
    client = MongoClient("mongodb://localhost:27017/")
    return client, client["olist"]


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
    client, db = get_db()

    sample = db.orders_embedded.find_one()
    order_id = sample["order_id"]
    graph_sample = db.orders_embedded.find_one({"items": {"$ne": []}})
    customer_id = graph_sample["customer_id"]

    results = {"system": "mongodb"}

    # ---------- OLTP baseline ----------
    def oltp_op():
        db.orders_embedded.update_one(
            {"order_id": order_id},
            {"$set": {"order_status": "delivered"}}
        )

    results["oltp"] = summarize(timed(oltp_op))
    print(f"OLTP -> {results['oltp']}")

    # ---------- OLAP baseline ----------
    def olap_op():
        list(db.orders_embedded.aggregate([
            {"$unwind": "$items"},
            {"$group": {"_id": "$customer_id", "total": {"$sum": "$items.price"}}},
            {"$sort": {"total": -1}},
            {"$limit": 10}
        ], allowDiskUse=True))

    results["olap"] = summarize(timed(olap_op))
    print(f"OLAP -> {results['olap']}")

    # ---------- Graph ($graphLookup) baseline ----------
    def graph_op():
        list(db.orders_embedded.aggregate([
            {"$match": {"customer_id": customer_id}},
            {"$unwind": "$items"},
            {"$group": {"_id": "$customer_id", "products": {"$addToSet": "$items.product_id"}}},
            {"$graphLookup": {
                "from": "orders_embedded",
                "startWith": "$products",
                "connectFromField": "products",
                "connectToField": "items.product_id",
                "as": "similar_customers",
                "maxDepth": 1
            }},
            {"$project": {"count": {"$size": "$similar_customers"}}}
        ], allowDiskUse=True))

    results["graph"] = summarize(timed(graph_op))
    print(f"Graph -> {results['graph']}")

    out_json = os.environ.get("RESULTS_OUT", "mongodb_baseline_results.json")
    with open(out_json, "w") as f:
        json.dump(results, f, indent=2)

    out_csv = out_json.replace(".json", ".csv")
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["system", "workload", "median_ms", "p99_ms", "count"])
        for wl in ("oltp", "olap", "graph"):
            m = results[wl]
            w.writerow(["mongodb", wl, m["median_ms"], m["p99_ms"], m["count"]])

    print(f"\nBaseline saved to {out_json} and {out_csv}")
    client.close()


if __name__ == "__main__":
    main()
