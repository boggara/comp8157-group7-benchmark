# PostgreSQL Pipeline — COMP 8157 Group 7

Owner: Sai Srinivas Uppara (110215983)

The PostgreSQL side of the benchmark: relational schema, ingestion, all three
workload types (OLTP / OLAP / graph-equivalent), the isolated baseline, the
index-scan-efficiency scaling test, and the co-scheduled worker used for the
interference-delta comparison. Output formats match the MongoDB scripts
(`oltp_threaded.py`, `scaling.py`, `mongodb_worker.py`) so results merge
cleanly across systems.

## Files

| File | Purpose |
|---|---|
| `postgres-docker-compose.yml` | PostgreSQL 15 container, same 2 CPU / 2 GB limits as the other systems |
| `pg_schema.sql` | Nine Olist tables with PK/FK constraints + workload indexes |
| `pg_ingest.py` | COPY-based ingestion, UTF-8, FK-safe load order, row-count + orphan validation, ends with `VACUUM ANALYZE` |
| `pg_oltp_threaded.py` | INSERT / UPDATE / point-lookup at 1/10/50/100 threads, p50/p95/p99 (connection pool + deadlock retry) |
| `pg_olap.py` | The four analytical queries from the proposal |
| `pg_recommendation.py` | Join/subquery equivalents of the three Cypher queries in `neo4j_queries.cypher` |
| `pg_scaling.py` | Point-lookup + aggregation latency at 10K / 50K / 107K orders |
| `pg_baseline.py` | Isolated baseline: 3 reps with warm-ups discarded, median + p99 + throughput, JSON + CSV |
| `pg_worker.py` | `run_workload(thread_count, duration_seconds)` — co-scheduled OLTP+OLAP+graph streams, same interface as `mongodb_worker.py` |

## Setup

```bash
docker compose -f postgres-docker-compose.yml up -d

# Place the Kaggle Olist CSVs in ./data/
# https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

pip install psycopg2-binary pandas numpy

# Apply schema, then load
PGPASSWORD=benchpass123 psql -h localhost -U bench -d olist -f pg_schema.sql
TEST_MODE=1 python pg_ingest.py   # 10K-order test slice first
python pg_ingest.py               # full 107K load (re-runnable; truncates first)
```

Connection settings default to `localhost:5432 / olist / bench / benchpass123`
and can be overridden with the `PG_DSN` environment variable.

## Run order for the benchmark phase

```bash
python pg_oltp_threaded.py     # -> pg_oltp_threaded_results.json
python pg_olap.py              # -> pg_olap_results.json
python pg_recommendation.py    # -> pg_recommendation_results.json
python pg_baseline.py          # -> pg_baseline_results.json / .csv  (isolated baseline)
python pg_scaling.py           # -> pg_scaling_results.json          (10K/50K/107K)
python pg_worker.py            # co-scheduled run; interference delta = worker vs baseline
```

## Methodology / modeling notes

- All nine tables keep their relational structure with FKs enforced, so the
  relational model is a faithful baseline rather than a flat dump.
- Client encoding is forced to UTF-8 (Olist text is Portuguese).
- `review_id` is not unique in the raw CSV, so `order_reviews` uses the
  `(review_id, order_id)` pair as its key and duplicates are dropped on load.
  `geolocation` has no natural key and gets a surrogate key with no FK.
- Undelivered orders (NULL `order_delivered_customer_date`) are filtered
  explicitly in the delivery-time aggregation.
- Every timed measurement is preceded by warm-up runs and medians are
  reported (per the proposal's noise-mitigation methodology); `VACUUM
  ANALYZE` runs after each load and between scaling steps so the planner has
  fresh statistics.
- At 100 threads the default connection limit is exhausted and update-heavy
  runs can deadlock; the harness uses a `ThreadedConnectionPool` and
  retry-with-backoff to handle both.
