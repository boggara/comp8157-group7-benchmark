"""
hot_partition_check.py
Owner: Alyan Khowaja (Cassandra role, Group 7)

Checks partition-size distribution across customer_state values in
orders_by_state to catch Cassandra's specific failure mode: a single
partition absorbing a disproportionate share of writes/reads (hot
partition), which causes coordinator saturation and latency spikes
under load.

Two checks:
  1. Static distribution check -- counts rows per state partition and
     flags any partition that exceeds a configurable share of total rows.
  2. Live check -- run this alongside oltp_benchmark.py at high
     concurrency (e.g. 100 threads) to see whether skew worsens under
     write pressure.

Usage:
    python hot_partition_check.py --threshold 0.30
"""

import argparse
from collections import Counter

from cassandra.cluster import Cluster

CASSANDRA_HOSTS = ["127.0.0.1"]
KEYSPACE = "olist_benchmark"
STATES = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "DF", "GO", "PE",
          "ES", "CE", "PA", "MT", "MA", "MS", "PB", "PI", "RN", "AL",
          "SE", "TO", "RO", "AM", "AC", "AP", "RR"]


def partition_row_counts(session) -> Counter:
    """COUNT(*) per partition. Each call is a single-partition scan --
    cheap and safe to run against a live cluster, unlike a full-table
    COUNT which would require ALLOW FILTERING across all partitions.
    """
    counts = Counter()
    for state in STATES:
        row = session.execute(
            "SELECT COUNT(*) AS c FROM orders_by_state WHERE customer_state = %s",
            (state,)
        ).one()
        if row and row.c:
            counts[state] = row.c
    return counts


def report(counts: Counter, threshold: float):
    total = sum(counts.values())
    if total == 0:
        print("No data found -- run cassandra_ingest.py first.")
        return

    print(f"{'State':<8}{'Rows':>10}{'Share':>10}")
    print("-" * 28)
    hot = []
    for state, count in counts.most_common():
        share = count / total
        flag = "  <-- HOT" if share > threshold else ""
        print(f"{state:<8}{count:>10,}{share:>9.1%}{flag}")
        if share > threshold:
            hot.append((state, share))

    print("-" * 28)
    print(f"{'TOTAL':<8}{total:>10,}")

    if hot:
        print(f"\nWARNING: {len(hot)} partition(s) exceed the {threshold:.0%} threshold:")
        for state, share in hot:
            print(f"  {state}: {share:.1%} of all rows -- coordinator saturation risk under load")
    else:
        print(f"\nNo partition exceeds the {threshold:.0%} threshold. "
              f"Distribution acceptable -- consistent with the Week 6 check "
              f"documented in the contribution log.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--threshold", type=float, default=0.30,
                         help="Fraction of total rows that flags a partition as hot (default 0.30)")
    parser.add_argument("--hosts", nargs="+", default=CASSANDRA_HOSTS)
    args = parser.parse_args()

    cluster = Cluster(args.hosts)
    session = cluster.connect(KEYSPACE)

    counts = partition_row_counts(session)
    report(counts, args.threshold)

    cluster.shutdown()


if __name__ == "__main__":
    main()
