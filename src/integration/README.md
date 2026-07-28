# Nagalakshmi Pravallika Kondapaturi - Integration Layer

## Course
COMP 8157 - Advanced Database Topics

## Group
Group 7

## Project Topic
Benchmarking Relational and NoSQL Databases Under Heterogeneous E-Commerce Workloads

## My Assigned Role
Integration Layer / Cross-System Benchmarking

## My Contribution

This folder contains my individual contribution for the project.

My responsibilities include:

- Docker Compose setup for PostgreSQL, MongoDB, Cassandra, and Neo4j
- Python concurrency scheduler
- Cross-system workload execution
- Metrics collection
- p50, p95, and p99 latency calculation
- Throughput calculation
- Interference delta analysis
- Index scan comparison
- Visualization of benchmark results

## Files

| File | Description |
|---|---|
| docker-compose-all.yml | Starts all four database containers |
| integration_scheduler.py | Runs OLTP, OLAP, and graph workloads concurrently |
| metrics.py | Collects latency and summary metrics |
| interference_delta.py | Calculates isolated vs concurrent workload performance difference |
| index_scan_all.py | Measures index scan latency at different dataset sizes |
| visualization.py | Generates benchmark graphs |

## How to Run

Start databases:

```bash
docker compose -f docker-compose-all.yml up -d