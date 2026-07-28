"""
COMP 8157 Group 7 - Neo4j isolated baseline benchmark
(built on top of Bhavana Volati's neo4j_ingest.py / neo4j_queries.cypher)

Runs the same OLTP / OLAP / graph queries as neo4j_worker.py, but
ONE AT A TIME (single stream), to get a clean isolated baseline for
the interference-delta comparison.
"""

import csv
import json
import os
import time

import numpy as np
from py2neo import Graph

NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "neo4jpass123")
REPS = int(os.environ.get("REPS", "20"))
WARMUPS = 2

OLTP_QUERY = """
MATCH (c:Customer {customer_id: $customerId})-[r:PURCHASED]->(p:Product {product_id: $productId})
SET r.status = 'delivered'
"""

OLAP_QUERY = """
MATCH (:Customer)-[:PURCHASED]->(p:Product)
RETURN p.category AS category, count(*) AS purchases
ORDER BY purchases DESC
LIMIT 10
"""

GRAPH_QUERY = """
MATCH (target:Customer {customer_id: $customerId})-[:PURCHASED]->(:Product)<-[:PURCHASED]-(other:Customer)-[:PURCHASED]->(rec:Product)
WHERE NOT (target)-[:PURCHASED]->(rec)
RETURN rec.product_id, count(*) AS strength
ORDER BY strength DESC
LIMIT 10
"""


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
    graph = Graph(NEO4J_URI, auth=NEO4J_AUTH)
    sample = graph.run(
        "MATCH (c:Customer)-[:PURCHASED]->(p:Product) RETURN c.customer_id AS cid, p.product_id AS pid LIMIT 1"
    ).data()[0]
    customer_id, product_id = sample["cid"], sample["pid"]

    results = {"system": "neo4j"}

    results["oltp"] = summarize(timed(
        lambda: graph.run(OLTP_QUERY, customerId=customer_id, productId=product_id)
    ))
    print(f"OLTP -> {results['oltp']}")

    results["olap"] = summarize(timed(lambda: list(graph.run(OLAP_QUERY))))
    print(f"OLAP -> {results['olap']}")

    results["graph"] = summarize(timed(
        lambda: list(graph.run(GRAPH_QUERY, customerId=customer_id))
    ))
    print(f"Graph -> {results['graph']}")

    out_json = os.environ.get("RESULTS_OUT", "neo4j_baseline_results.json")
    with open(out_json, "w") as f:
        json.dump(results, f, indent=2)

    out_csv = out_json.replace(".json", ".csv")
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["system", "workload", "median_ms", "p99_ms", "count"])
        for wl in ("oltp", "olap", "graph"):
            m = results[wl]
            w.writerow(["neo4j", wl, m["median_ms"], m["p99_ms"], m["count"]])

    print(f"\nBaseline saved to {out_json} and {out_csv}")


if __name__ == "__main__":
    main()
