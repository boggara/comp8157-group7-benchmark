"""
olap_queries.py
Owner: Alyan Khowaja (Cassandra role, Group 7)

Four OLAP rollup queries for Cassandra, written within CQL's constraints
(no server-side joins, no arbitrary GROUP BY / ORDER BY across
partitions). Where CQL can't express the aggregation directly, the
partition is fetched and the aggregation finishes client-side in Python
-- this is the trade-off documented in the proposal's System Design
section.

Queries:
  1. Total revenue grouped by customer state
  2. Average delivery time per seller region (pre-aggregated table)
  3. Top customers by order count within a state partition
  4. Freight cost distribution bucketed by month

Usage:
    python olap_queries.py --query revenue --state SP
    python olap_queries.py --query delivery --state RJ
    python olap_queries.py --query top-customers --state SP --n 10
    python olap_queries.py --query freight --state SP
"""

import argparse
from collections import defaultdict

from cassandra.cluster import Cluster

CASSANDRA_HOSTS = ["127.0.0.1"]
KEYSPACE = "olist_benchmark"


def revenue_by_state(session, state: str):
    """Query 1: total revenue for a customer_state partition.

    Single-partition scan, summed client-side since CQL has no SUM()
    over arbitrary result sets without server-side aggregation support
    on non-numeric-only clustering.
    """
    rows = session.execute(
        "SELECT payment_value FROM orders_by_state WHERE customer_state = %s",
        (state,)
    )
    total = sum((r.payment_value or 0) for r in rows)
    print(f"Total revenue for {state}: {total}")
    return total


def avg_delivery_by_seller_region(session, seller_state: str):
    """Query 2: average delivery time (days) for a seller_state partition,
    read from the pre-aggregated seller_region_delivery table so this
    stays a single-partition query instead of a full-table scan.
    """
    rows = session.execute(
        "SELECT delivery_days FROM seller_region_delivery WHERE seller_state = %s",
        (seller_state,)
    )
    days = [r.delivery_days for r in rows if r.delivery_days is not None]
    avg = sum(days) / len(days) if days else 0
    print(f"Average delivery time for seller_state={seller_state}: {avg:.2f} days over {len(days)} orders")
    return avg


def top_customers_by_state(session, state: str, n: int = 10):
    """Query 3: top-N customers by order count within a state partition.

    CQL counter tables can't ORDER BY the counter value, so the whole
    partition is pulled and sorted client-side. This only works because
    a single-state partition is bounded in size; documented as a CQL
    constraint in the contribution log.
    """
    rows = session.execute(
        "SELECT customer_id, order_count FROM customer_order_counts WHERE customer_state = %s",
        (state,)
    )
    ranked = sorted(rows, key=lambda r: r.order_count, reverse=True)[:n]
    for r in ranked:
        print(f"  {r.customer_id}: {r.order_count} orders")
    return ranked


def freight_distribution_by_month(session, state: str):
    """Query 4: freight cost distribution bucketed by rolling monthly
    windows, read from the pre-bucketed freight_by_month table.
    """
    rows = session.execute(
        "SELECT year_month, freight_value FROM freight_by_month WHERE customer_state = %s",
        (state,)
    )
    buckets = defaultdict(list)
    for r in rows:
        buckets[r.year_month].append(float(r.freight_value or 0))

    print(f"Freight distribution for {state}:")
    for ym in sorted(buckets):
        vals = buckets[ym]
        avg = sum(vals) / len(vals)
        print(f"  {ym}: n={len(vals):4d}  avg_freight={avg:6.2f}  "
              f"min={min(vals):6.2f}  max={max(vals):6.2f}")
    return buckets


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", required=True,
                         choices=["revenue", "delivery", "top-customers", "freight"])
    parser.add_argument("--state", required=True, help="Two-letter state code, e.g. SP")
    parser.add_argument("--n", type=int, default=10, help="Top-N for top-customers query")
    parser.add_argument("--hosts", nargs="+", default=CASSANDRA_HOSTS)
    args = parser.parse_args()

    cluster = Cluster(args.hosts)
    session = cluster.connect(KEYSPACE)

    if args.query == "revenue":
        revenue_by_state(session, args.state)
    elif args.query == "delivery":
        avg_delivery_by_seller_region(session, args.state)
    elif args.query == "top-customers":
        top_customers_by_state(session, args.state, args.n)
    elif args.query == "freight":
        freight_distribution_by_month(session, args.state)

    cluster.shutdown()


if __name__ == "__main__":
    main()
