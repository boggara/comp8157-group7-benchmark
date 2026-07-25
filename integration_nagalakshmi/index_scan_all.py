"""
COMP 8157 Group 7 - Integration layer
Nagalakshmi Pravallika Kondapaturi - Docker orchestration & integration

Consolidates the four real per-database scaling results
(pg_scaling_results.json, mongodb scaling_results.json,
cassandra_scaling_results.json, neo4j_scaling_results.json) into one
CSV so index_degradation charts can plot all four systems together.

This replaces the earlier placeholder version of this file, which
used time.sleep(records / <divisor>) to fabricate latency numbers per
database instead of reading real measured results. Run the four
*_scaling.py scripts first (each one measures its own database at
10K / 50K / 107K records), then run this script.

Usage:
    python pg_scaling.py
    python scaling.py          # mongo
    python cassandra_scaling.py
    python neo4j_scaling.py
    python integration_nagalakshmi/index_scan_all.py
"""

import csv
import json
from pathlib import Path

SCALE_LABEL_TO_RECORDS = {"10K": 10000, "50K": 50000, "107K": 107000}

SOURCES = {
    "PostgreSQL": "pg_scaling_results.json",
    "MongoDB": "scaling_results.json",
    "Cassandra": "cassandra_scaling_results.json",
    "Neo4j": "neo4j_scaling_results.json",
}


def main():
    repo_root = Path(__file__).resolve().parent.parent
    results_dir = Path(__file__).resolve().parent / "results"
    results_dir.mkdir(exist_ok=True)

    rows = []
    missing = []
    for db_name, filename in SOURCES.items():
        path = repo_root / filename
        if not path.exists():
            missing.append(f"{db_name} ({filename})")
            continue

        with open(path) as f:
            data = json.load(f)

        for label, records in SCALE_LABEL_TO_RECORDS.items():
            if label not in data:
                continue
            rows.append({
                "database": db_name,
                "records": records,
                "point_lookup_ms": data[label]["point_lookup_ms"],
                "index_scan_latency_ms": data[label]["aggregation_ms"],
            })

    out_path = results_dir / "index_scan_results.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "database", "records", "point_lookup_ms", "index_scan_latency_ms"
        ])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Index scan results written to {out_path}")
    if missing:
        print("\nWARNING: missing per-database scaling results, run these first:")
        for m in missing:
            print(f"  - {m}")


if __name__ == "__main__":
    main()
