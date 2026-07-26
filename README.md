# Benchmarking Relational and NoSQL Databases Under Heterogeneous E-Commerce Workloads

**COMP.8157 – Advanced Database Topics | Term 2026S | Instructor: Dr. Andreas S. Maniatis**
**Group 7:** Bhavana Volati · Mounika Boggarapu · Sai Srinivas Uppara · Alyan Khowaja · Nagalakshmi Pravallika Kondapaturi

A benchmark comparing PostgreSQL (relational), MongoDB (document), Apache Cassandra (wide-column), and Neo4j (graph) under concurrent OLTP, OLAP, and graph-traversal workloads, using the Olist Brazilian E-Commerce dataset (99,441 orders). Full background, requirements, and design rationale are in the D.4.1 and D.4.2 documents.

## Getting started

New to this repository? Start with **[RUNBOOK.md](RUNBOOK.md)** for the exact commands to bring up the four databases, load the data, and reproduce the results — or see the D.4.4 Deployment Document for the fuller writeup.

## Repository layout

Each database's ingestion, workload, baseline, and scaling scripts are owned by one team member and follow the same naming pattern across systems (`*_worker.py` = co-scheduled run, `*_baseline.py` = isolated run, `*_scaling.py` = index-scan-at-scale).

### PostgreSQL — Sai Srinivas Uppara

| File | Purpose |
|---|---|
| `pg_schema.sql` | Nine Olist tables with PK/FK constraints + indexes |
| `pg_ingest.py` | COPY-based ingestion with row-count + orphan validation |
| `pg_oltp_threaded.py`, `pg_olap.py`, `pg_recommendation.py` | Individual workload benchmarks (OLTP, OLAP, graph-equivalent recommendation queries) |
| `pg_worker.py` | Co-scheduled OLTP+OLAP+graph run, used by the integration layer |
| `pg_baseline.py` | Isolated baseline (median, p99, throughput) |
| `pg_scaling.py` | Point-lookup + aggregation latency at 10K/50K/107K orders |
| `README_postgres.md` | Full setup notes and modeling decisions for this pipeline |

### MongoDB — Mounika Boggarapu

| File | Purpose |
|---|---|
| `ingest_embedded.py` | Loads Olist orders as embedded documents (the model used for all benchmark results) |
| `ingest.py` | Alternate flat-collection loader, kept for the flat-vs-embedded comparison referenced in D.3.1; not used by the live benchmark run |
| `indexes.py` | Index creation |
| `oltp_threaded.py`, `olap_correct.py`, `graph_copurchase.py` | Individual workload benchmarks (OLTP, OLAP, `$graphLookup`-based co-purchase) |
| `mongodb_worker.py` | Co-scheduled OLTP+OLAP+graph run |
| `mongodb_baseline.py` | Isolated baseline |
| `scaling.py` | Point-lookup + aggregation latency at 10K/50K/107K orders |

### Cassandra — Alyan Khowaja

| File | Purpose |
|---|---|
| `schema.cql` | Wide-row tables, partitioned by customer state |
| `cassandra_ingest.py` | Ingestion into the partition-key-driven schema |
| `hot_partition_check.py` | Verifies even data distribution across partitions before high-concurrency writes |
| `oltp_benchmark.py`, `olap_queries.py` | Individual workload benchmarks |
| `cassandra_worker.py` | Co-scheduled OLTP+OLAP+graph run |
| `cassandra_baseline.py` | Isolated baseline |
| `cassandra_scaling.py` | Point-lookup + aggregation latency at 10K/50K/107K orders |

### Neo4j — Bhavana Volati

| File | Purpose |
|---|---|
| `neo4j_ingest.py` | Loads customers/products/sellers as nodes with purchase/sale edges |
| `neo4j_queries.cypher` | Co-purchase, seller-network, and customer-similarity Cypher queries |
| `neo4j_worker.py` | Co-scheduled OLTP+OLAP+graph run |
| `neo4j_baseline.py` | Isolated baseline |
| `neo4j_scaling.py` | Point-lookup + aggregation latency at 10K/50K/107K orders |

### Integration & orchestration — Nagalakshmi Pravallika Kondapaturi

See [`integration_nagalakshmi/`](integration_nagalakshmi/):

| File | Purpose |
|---|---|
| `docker-compose-all.yml` | All four databases, one file, identical CPU/memory limits per container |
| `integration_scheduler.py` | Runs every database's isolated baseline, then the co-scheduled suite at concurrency 1/10/50/100 |
| `interference_delta.py` | Computes the isolated-vs-concurrent interference delta — the project's central result |
| `index_scan_all.py` | Consolidates all four systems' scaling results into one CSV |
| `visualization.py` | Generates the comparison charts |
| `results/` | Output CSVs: `isolated_summary.csv`, `summary_results.csv`, `interference_delta.csv`, `index_scan_results.csv` |
| `graphs/` | Output charts: `latency_distribution.png`, `index_degradation.png` |

### Root

- `requirements.txt` — Python dependencies
- `RUNBOOK.md` — step-by-step commands to reproduce every result in this repository

## Documentation

- D.4.1 — User Requirements and Analysis
- D.4.2 — Design Document
- D.4.4 — Deployment Document
- D.4.5 — User Guide
