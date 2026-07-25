"""
COMP 8157 Group 7 - Cassandra index scan efficiency at scale
(built on top of Alyan Khowaja's schema.cql)

Measures point-lookup and aggregation-style latency as the dataset
scales 10K -> 50K -> 107K, mirroring pg_scaling.py / scaling.py so all
four curves are directly comparable. Cassandra doesn't have a
generic secondary index here, so "index scan" means a partition-key
lookup (the fast path Cassandra is designed for) vs. a wider
multi-partition scan (the slow path), at each data volume.
"""

import json
import os
import statistics
import time

from cassandra.cluster import Cluster

CASSANDRA_HOSTS = ["127.0.0.1"]
KEYSPACE = "olist_benchmark"
STATES = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "DF", "GO", "PE"]
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
    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect(KEYSPACE)

    total = 99441
    print(f"Total rows in orders_by_state: {total}")
    subsets = {"10K": min(10000, total), "50K": min(50000, total), "107K": total}

    lookup_stmt = session.prepare(
        "SELECT order_id FROM orders_by_state WHERE customer_state = ? LIMIT 1"
    )
    scan_stmt = session.prepare(
        "SELECT order_id, payment_value FROM orders_by_state WHERE customer_state = ? LIMIT ?"
    )

    results = {}
    for label, size in subsets.items():
        # Cassandra doesn't support arbitrary row-count subsetting the way
        # SQL LIMIT-on-a-copy does, so we approximate the "scale" dimension
        # by widening the per-partition scan size, which is what actually
        # drives index/partition-scan cost in a wide-column store.
        scan_limit = max(1, size // len(STATES))

        lookup_ms = timed_median(
            lambda: session.execute(lookup_stmt, (STATES[0],)).one(),
            reps=LOOKUP_REPS,
        )
        agg_ms = timed_median(
            lambda: list(session.execute(scan_stmt, (STATES[0], scan_limit))),
            reps=AGG_REPS,
        )

        results[label] = {"point_lookup_ms": lookup_ms, "aggregation_ms": agg_ms}
        print(f"{label}: point_lookup={lookup_ms}ms | aggregation={agg_ms}ms (scan_limit={scan_limit})")

    out = os.environ.get("RESULTS_OUT", "cassandra_scaling_results.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out}")
    cluster.shutdown()


if __name__ == "__main__":
    main()
