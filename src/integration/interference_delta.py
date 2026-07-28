"""
COMP 8157 Group 7 - Integration layer
Nagalakshmi Pravallika Kondapaturi - Docker orchestration & integration

Computes the interference delta: the percent change between a
workload's isolated (single-stream) latency and its co-scheduled
latency at each concurrency level. This is the project's central
methodological contribution per the design doc (Section 4, FR-7).

Reads:
    results/isolated_summary.csv   (one row per database/workload)
    results/summary_results.csv    (one row per database/workload/concurrency)
Writes:
    results/interference_delta.csv
"""

import csv
from pathlib import Path


def calculate_delta(isolated_file, concurrent_file, output_file):
    isolated = {}
    with open(isolated_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["database"], row["workload"])
            isolated[key] = float(row["avg_ms"])

    concurrent_rows = []
    with open(concurrent_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            concurrent_rows.append(row)

    Path(output_file).parent.mkdir(exist_ok=True)

    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "database",
            "workload",
            "concurrency",
            "isolated_avg_ms",
            "concurrent_avg_ms",
            "interference_delta_ms",
            "interference_percent",
        ])
        writer.writeheader()

        for row in concurrent_rows:
            key = (row["database"], row["workload"])
            if key not in isolated:
                continue

            isolated_ms = isolated[key]
            concurrent_ms = float(row["avg_ms"])
            delta = concurrent_ms - isolated_ms
            percent = (delta / isolated_ms) * 100 if isolated_ms != 0 else 0

            writer.writerow({
                "database": row["database"],
                "workload": row["workload"],
                "concurrency": row["concurrency"],
                "isolated_avg_ms": round(isolated_ms, 3),
                "concurrent_avg_ms": round(concurrent_ms, 3),
                "interference_delta_ms": round(delta, 3),
                "interference_percent": round(percent, 2),
            })


if __name__ == "__main__":
    calculate_delta(
        "results/isolated_summary.csv",
        "results/summary_results.csv",
        "results/interference_delta.csv",
    )
    print("Interference delta calculated -> results/interference_delta.csv")
