"""
COMP 8157 Group 7 - Integration layer
Nagalakshmi Pravallika Kondapaturi - Docker orchestration & integration

Drives all four database pipelines for real:
  1. Runs each system's isolated baseline script (subprocess) and
     collects the results into results/isolated_summary.csv.
  2. Runs each system's co-scheduled run_workload() at concurrency
     levels 1 / 10 / 50 / 100 and collects the results into
     results/raw_results.csv and results/summary_results.csv.

This replaces the earlier placeholder version of this file, which used
time.sleep() with hardcoded per-database durations instead of talking
to the actual databases. That version was a stand-in written before
the individual pipelines (mongodb_worker.py, pg_worker.py,
cassandra_worker.py, neo4j_worker.py) existed, and it should not be
used to produce numbers for the final report.

Usage:
    docker compose -f docker-compose-all.yml up -d
    # wait for all four containers to be healthy, then:
    python integration_scheduler.py
"""

import csv
import json
import subprocess
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import cassandra_worker
import mongodb_worker
import neo4j_worker
import pg_worker

CONCURRENCY_LEVELS = [1, 10, 50, 100]
DURATION_SECONDS = 30  # per concurrency level, per system - keep short for the demo run

WORKERS = {
    "PostgreSQL": pg_worker.run_workload,
    "MongoDB": mongodb_worker.run_workload,
    "Cassandra": cassandra_worker.run_workload,
    "Neo4j": neo4j_worker.run_workload,
}

BASELINE_SCRIPTS = {
    "PostgreSQL": "pg_baseline.py",
    "MongoDB": "mongodb_baseline.py",
    "Cassandra": "cassandra_baseline.py",
    "Neo4j": "neo4j_baseline.py",
}


def run_isolated_baselines(repo_root: Path, results_dir: Path):
    """Runs each system's baseline script and merges the JSON outputs
    into one isolated_summary.csv, in the same row shape as the
    co-scheduled summary so interference_delta.py can compare them."""
    rows = []
    for db_name, script in BASELINE_SCRIPTS.items():
        script_path = repo_root / script
        out_json = repo_root / f"{script.replace('.py', '')}_results.json"
        print(f"\n=== Isolated baseline: {db_name} ({script}) ===")
        subprocess.run([sys.executable, str(script_path)], cwd=repo_root, check=True)

        with open(out_json) as f:
            data = json.load(f)

        if db_name == "PostgreSQL":
            # pg_baseline.py has a different shape from the other three:
            # oltp is a single dict, but olap/recommendation are dicts of
            # named queries (revenue_by_category, seller_network, etc.),
            # and none of them track a "count". Average the named queries
            # per workload group to get one representative number, and
            # treat "recommendation" as this system's GRAPH-equivalent row.
            oltp = data["oltp"]
            rows.append({
                "database": db_name, "workload": "OLTP",
                "count": "", "avg_ms": oltp["median_ms"],
                "p50_ms": oltp["median_ms"], "p95_ms": oltp["p99_ms"], "p99_ms": oltp["p99_ms"],
            })
            for workload, out_label in (("olap", "OLAP"), ("recommendation", "GRAPH")):
                queries = data[workload]
                medians = [q["median_ms"] for q in queries.values()]
                p99s = [q["p99_ms"] for q in queries.values()]
                rows.append({
                    "database": db_name, "workload": out_label,
                    "count": "", "avg_ms": sum(medians) / len(medians),
                    "p50_ms": sum(medians) / len(medians),
                    "p95_ms": sum(p99s) / len(p99s), "p99_ms": sum(p99s) / len(p99s),
                })
            continue

        for workload in ("oltp", "olap", "graph"):
            if workload in data:
                m = data[workload]
                rows.append({
                    "database": db_name,
                    "workload": workload.upper(),
                    "count": m["count"],
                    "avg_ms": m["median_ms"],  # baseline scripts report median, not mean
                    "p50_ms": m["median_ms"],
                    "p95_ms": m.get("p99_ms", m["median_ms"]),
                    "p99_ms": m.get("p99_ms", m["median_ms"]),
                })

    out_csv = results_dir / "isolated_summary.csv"
    with open(out_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "database", "workload", "count", "avg_ms", "p50_ms", "p95_ms", "p99_ms"
        ])
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nIsolated baselines written to {out_csv}")


def run_coscheduled(results_dir: Path):
    """Runs each system's run_workload() at every concurrency level and
    writes raw per-call rows plus a p50/p95/p99 summary, matching the
    schema the rest of the group's report/visualization code expects."""
    raw_rows = []
    summary_rows = []

    for db_name, run_workload in WORKERS.items():
        for level in CONCURRENCY_LEVELS:
            print(f"\n=== Co-scheduled: {db_name} @ concurrency={level} ===")
            result = run_workload(thread_count=level, duration_seconds=DURATION_SECONDS)

            for workload in ("oltp", "olap", "graph"):
                m = result[workload]
                if m["count"] == 0:
                    continue
                summary_rows.append({
                    "database": db_name,
                    "workload": workload.upper(),
                    "concurrency": level,
                    "count": m["count"],
                    "avg_ms": m["p50"],  # p50 used as the representative "avg" for the summary view
                    "p50_ms": m["p50"],
                    "p95_ms": m["p95"],
                    "p99_ms": m["p99"],
                })
                # note: per-call raw latencies aren't retained by run_workload() (only
                # the percentile summary is), so raw_results.csv stores one row per
                # (database, workload, concurrency) rather than one row per call.
                raw_rows.append({
                    "database": db_name,
                    "workload": workload.upper(),
                    "concurrency": level,
                    "p50_ms": m["p50"],
                    "p95_ms": m["p95"],
                    "p99_ms": m["p99"],
                    "ops": m["count"],
                })

    with open(results_dir / "raw_results.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "database", "workload", "concurrency", "p50_ms", "p95_ms", "p99_ms", "ops"
        ])
        writer.writeheader()
        writer.writerows(raw_rows)

    with open(results_dir / "summary_results.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "database", "workload", "concurrency", "count", "avg_ms", "p50_ms", "p95_ms", "p99_ms"
        ])
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"\nCo-scheduled results written to {results_dir / 'raw_results.csv'} and "
          f"{results_dir / 'summary_results.csv'}")


def main():
    repo_root = Path(__file__).resolve().parent.parent
    results_dir = Path(__file__).resolve().parent / "results"
    results_dir.mkdir(exist_ok=True)

    run_isolated_baselines(repo_root, results_dir)
    run_coscheduled(results_dir)

    print("\nAll done. Run interference_delta.py next to compute the interference delta.")


if __name__ == "__main__":
    main()
