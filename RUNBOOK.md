# What changed and why

Your team's four database pipelines (MongoDB, PostgreSQL, Cassandra, Neo4j
ingestion/OLTP/OLAP/graph scripts) are real — they use the actual drivers
and connect to real containers. That part is fine.

The problem was specifically **Nagalakshmi's integration layer**
(`integration_nagalakshmi/`), which is what's supposed to produce the
three "remaining work" items from your D.3.1 report:

- `integration_scheduler.py` was using `time.sleep(0.05)` etc. per
  database instead of calling any real database.
- `index_scan_all.py` was using `time.sleep(records / 1_000_000)` instead
  of running real queries.
- `interference_delta.py` was correct code, but had nothing real to
  compare — there was no `isolated_summary.csv` for it to read.

None of the CSVs currently in `integration_nagalakshmi/results/` are real
measurements. They should not go in the final report as-is.

## What I fixed / added

| File | Status |
|---|---|
| `mongodb_worker.py` | fixed — had an indentation bug that made it a syntax error |
| `mongodb_baseline.py` | **new** — isolated baseline (Mongo didn't have one; Postgres already did) |
| `cassandra_worker.py` | **new** — co-scheduled OLTP/OLAP/graph-equivalent worker |
| `cassandra_baseline.py` | **new** — isolated baseline |
| `cassandra_scaling.py` | **new** — index-scan-at-scale (10K/50K/107K) |
| `neo4j_worker.py` | **new** — co-scheduled OLTP/OLAP/graph worker |
| `neo4j_baseline.py` | **new** — isolated baseline |
| `neo4j_scaling.py` | **new** — index-scan-at-scale |
| `integration_nagalakshmi/integration_scheduler.py` | **rewritten** — now imports and actually calls all four real `run_workload()` functions, and runs all four baseline scripts as subprocesses |
| `integration_nagalakshmi/interference_delta.py` | **rewritten** — same idea as before, but now handles the concurrency column that real data has |
| `integration_nagalakshmi/index_scan_all.py` | **rewritten** — consolidates the four real `*_scaling_results.json` files instead of faking numbers |

All new files use the exact connection details already in your repo
(same hosts, ports, credentials, schema) so they should just work once
the containers are up.

## Order to run things tonight

```bash
# 1. Start all four databases
docker compose -f integration_nagalakshmi/docker-compose-all.yml up -d
# wait ~30s for them to be healthy

# 2. Make sure each database is already ingested at full scale (107K).
#    You should already have this from earlier work — if not, re-run
#    each team member's ingest.py / pg_ingest.py / cassandra_ingest.py /
#    neo4j_ingest.py first.

# 3. Index-scan-at-scale, one script per database (each writes its own
#    <db>_scaling_results.json in the repo root):
python pg_scaling.py
python scaling.py              # mongo
python cassandra_scaling.py
python neo4j_scaling.py

# 4. Consolidate the four scaling files into one CSV:
python integration_nagalakshmi/index_scan_all.py

# 5. Run the integration scheduler. This runs each database's isolated
#    baseline, then each database's co-scheduled workload at
#    concurrency 1/10/50/100 (30s per level, 4 levels x 4 DBs = ~8 min
#    of actual runtime, plus baselines). This is the long step.
python integration_nagalakshmi/integration_scheduler.py

# 6. Compute the interference delta:
python integration_nagalakshmi/interference_delta.py

# 7. Regenerate charts from the real results:
python integration_nagalakshmi/visualization.py
```

## Before you commit

- Delete or clearly relabel the old `benchmark_results.json` /
  `graph_copurchase_results.json` etc. at the repo root if they were
  ever produced by placeholder code — check each one's generating
  script the same way we checked the integration layer.
- If runtime is tight, you can lower `DURATION_SECONDS` in
  `integration_scheduler.py` (currently 30) — just say so in the report
  so the numbers are reproducible.
- Loop in Nagalakshmi before overwriting her files in the shared repo —
  she owns that folder and may already be aware of this or have WIP
  locally.
