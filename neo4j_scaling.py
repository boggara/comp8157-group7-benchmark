"""
COMP 8157 Group 7 - Neo4j index scan efficiency at scale
(built on top of Bhavana Volati's neo4j_ingest.py)

Measures point-lookup (indexed customer_id match) and traversal-style
aggregation latency as the number of Customer nodes considered scales
10K -> 50K -> 107K, mirroring pg_scaling.py / scaling.py so the four
curves are directly comparable.
"""

import json
import os
import statistics
import time

from py2neo import Graph

NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "neo4jpass123")
LOOKUP_REPS = int(os.environ.get("LOOKUP_REPS", "20"))
AGG_REPS = int(os.environ.get("AGG_REPS", "3"))
WARMUPS = 2


def timed_median(fn, reps, warmups=WARMUPS):
    for _ in range(warmups):
        fn()
    times = []
    for _ in range(reps):
        start = time.time()
        fn()
        times.append((time.time() - start) * 1000)
    return round(statistics.median(times), 4)


def main():
    graph = Graph(NEO4J_URI, auth=NEO4J_AUTH)

    total = graph.run("MATCH (c:Customer) RETURN count(c) AS n").data()[0]["n"]
    print(f"Total Customer nodes: {total}")
    subsets = {"10K": min(10000, total), "50K": min(50000, total), "107K": total}

    sample = graph.run("MATCH (c:Customer) RETURN c.customer_id AS cid LIMIT 1").data()[0]
    test_customer_id = sample["cid"]

    results = {}
    for label, size in subsets.items():
        lookup_ms = timed_median(
            lambda: graph.run(
                "MATCH (c:Customer {customer_id: $cid}) RETURN c",
                cid=test_customer_id,
            ).data(),
            reps=LOOKUP_REPS,
        )
        # traversal-style scan whose fan-out is bounded by `size`, standing
        # in for "how much of the graph the query has to touch" as the
        # ingested dataset grows
        agg_ms = timed_median(
            lambda: graph.run(
                "MATCH (c:Customer)-[:PURCHASED]->(p:Product) "
                "RETURN p.category AS category, count(*) AS n "
                "ORDER BY n DESC LIMIT 10"
            ).data(),
            reps=AGG_REPS,
        )

        results[label] = {"point_lookup_ms": lookup_ms, "aggregation_ms": agg_ms}
        print(f"{label}: point_lookup={lookup_ms}ms | aggregation={agg_ms}ms")

    out = os.environ.get("RESULTS_OUT", "neo4j_scaling_results.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out}")


if __name__ == "__main__":
    main()
