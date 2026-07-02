# Cassandra Pipeline — Group 7 (Olist Benchmarking Project)

**Owner:** Alyan Khowaja — Cassandra, wide-column model
**Course:** COMP.8157, Advanced Database Topics

This is the Cassandra deliverable for the group's database benchmarking
project: schema + scripts, matching the role card (Design schema →
OLTP scripts → OLAP queries → hot-partition check → baseline benchmark).

## Files

| File | Purpose |
|---|---|
| `schema.cql` | Keyspace + 4 tables: `orders_by_state` (main wide-row table), `seller_region_delivery`, `customer_order_counts` (counter table), `freight_by_month` |
| `cassandra_ingest.py` | Loads Olist CSVs → joins in pandas → bulk-loads via `execute_concurrent_with_args()` |
| `oltp_benchmark.py` | Insert + update workloads at concurrency 1/10/50/100, reports p50/p95/p99 latency + throughput |
| `olap_queries.py` | The 4 OLAP rollups: revenue by state, avg delivery by seller region, top customers, freight distribution by month |
| `hot_partition_check.py` | Row-count-per-partition check to catch write skew (Cassandra's specific failure mode) |

## How to run (against your Docker Cassandra 4.1 container)

```bash
pip install -r requirements.txt

# 1. Create schema
cqlsh -f schema.cql

# 2. Ingest data (start small to validate, then scale up)
python cassandra_ingest.py --data-dir ./olist_csv --limit 10000
python cassandra_ingest.py --data-dir ./olist_csv --limit 50000
python cassandra_ingest.py --data-dir ./olist_csv                 # full 107K

# 3. Hot-partition check after ingestion
python hot_partition_check.py --threshold 0.30

# 4. Isolated baseline OLTP benchmark (run insert and update separately)
python oltp_benchmark.py --workload insert --ops 2000 --out results/oltp_insert.csv
python oltp_benchmark.py --workload update --ops 2000 --out results/oltp_update.csv

# 5. OLAP queries
python olap_queries.py --query revenue --state SP
python olap_queries.py --query delivery --state RJ
python olap_queries.py --query top-customers --state SP --n 10
python olap_queries.py --query freight --state SP
```

## Design decisions (for the final report's Cassandra section)

- **Partition key = `customer_state`**: chosen for query-pattern
  alignment (state-level revenue/delivery dashboards are the primary
  read pattern) and because 27 states gives enough cardinality to
  avoid a single dominant partition, while staying analytically
  meaningful (unlike, say, `order_id`, which would make range/rollup
  queries impossible).
- **Clustering key = `order_purchase_timestamp` (DESC)**: supports the
  "recent orders per state" and "time-windowed rollup" access patterns
  without `ALLOW FILTERING`.
- **Two-table split for seller-side rollups**: `seller_region_delivery`
  is a separate partition-by-`seller_state` table so the delivery-time
  OLAP query never needs a cross-partition scan.
- **Counter table for order counts**: `customer_order_counts` isolates
  the increment-heavy counter type from the main table (Cassandra
  counters can't share a table with regular columns cleanly).
- **Client-side sort for top-N**: CQL has no `ORDER BY` on non-clustering
  or counter columns, so `top_customers_by_state()` pulls the bounded
  partition and finishes the ranking in Python — documented as a CQL
  constraint rather than a workaround.

## Status vs. contribution log

Matches Weeks 4–7 of the D.3.2 log: Docker container + driver
connection (Week 4), ingestion script + 10K test (Week 5), full 107K
load + OLTP scripts + hot-partition check (Week 6), OLAP queries +
isolated baseline benchmark (Week 7). Ready for the co-scheduled
interference-measurement phase (Phase 3 of the project timeline).
