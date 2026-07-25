"""
COMP 8157 Group 7 - Neo4j co-scheduled workload worker
(built on top of Bhavana Volati's neo4j_ingest.py / neo4j_queries.cypher)

Runs OLTP + OLAP + graph (Cypher co-purchase) streams SIMULTANEOUSLY
against Neo4j for a fixed duration. Exposes run_workload(thread_count,
duration_seconds) with the same signature and return shape as the
other three workers, so the integration harness can drive all four
systems identically.

"OLTP" for a graph store means writing/updating a relationship
(order status style update modeled as a property write on PURCHASED).
"OLAP" is a category-level rollup over Product nodes. "Graph" is the
real multi-hop co-purchase query from neo4j_queries.cypher.
"""

import threading
import time

import numpy as np
from py2neo import Graph

NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "neo4jpass123")

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


def run_workload(thread_count, duration_seconds=30):
    """Runs all 3 workload types simultaneously for a given duration.
    Called by the group integration harness."""
    graph = Graph(NEO4J_URI, auth=NEO4J_AUTH)

    # sample a customer/product pair that actually has a PURCHASED edge
    sample = graph.run(
        "MATCH (c:Customer)-[:PURCHASED]->(p:Product) RETURN c.customer_id AS cid, p.product_id AS pid LIMIT 1"
    ).data()[0]
    customer_id, product_id = sample["cid"], sample["pid"]

    results = {"oltp": [], "olap": [], "graph": []}
    stop_event = threading.Event()
    lock = threading.Lock()

    def oltp_worker():
        while not stop_event.is_set():
            start = time.time()
            graph.run(OLTP_QUERY, customerId=customer_id, productId=product_id)
            end = time.time()
            with lock:
                results["oltp"].append((end - start) * 1000)

    def olap_worker():
        while not stop_event.is_set():
            start = time.time()
            list(graph.run(OLAP_QUERY))
            end = time.time()
            with lock:
                results["olap"].append((end - start) * 1000)

    def graph_worker():
        while not stop_event.is_set():
            start = time.time()
            list(graph.run(GRAPH_QUERY, customerId=customer_id))
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
    print("Running Neo4j co-scheduled workload (30 seconds per level)...")
    for t in [1, 10, 50, 100]:
        print(f"\nThread count: {t}")
        r = run_workload(thread_count=t, duration_seconds=30)
        for k in ("oltp", "olap", "graph"):
            m = r[k]
            print(f"  {k.upper():5s} -> p50={m['p50']}ms p95={m['p95']}ms p99={m['p99']}ms ops={m['count']}")
    print("\nDone. The integration harness can import run_workload() from this script.")
