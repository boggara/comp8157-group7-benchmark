import csv
from pathlib import Path


def calculate_delta(isolated_file, concurrent_file, output_file):
    isolated = {}
    concurrent = {}

    with open(isolated_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["database"], row["workload"])
            isolated[key] = float(row["avg_ms"])

    with open(concurrent_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["database"], row["workload"])
            concurrent[key] = float(row["avg_ms"])

    Path("results").mkdir(exist_ok=True)

    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "database",
            "workload",
            "isolated_avg_ms",
            "concurrent_avg_ms",
            "interference_delta_ms",
            "interference_percent"
        ])

        writer.writeheader()

        for key in isolated:
            if key in concurrent:
                isolated_ms = isolated[key]
                concurrent_ms = concurrent[key]
                delta = concurrent_ms - isolated_ms
                percent = (delta / isolated_ms) * 100 if isolated_ms != 0 else 0

                writer.writerow({
                    "database": key[0],
                    "workload": key[1],
                    "isolated_avg_ms": round(isolated_ms, 3),
                    "concurrent_avg_ms": round(concurrent_ms, 3),
                    "interference_delta_ms": round(delta, 3),
                    "interference_percent": round(percent, 2)
                })


if __name__ == "__main__":
    calculate_delta(
        "results/isolated_summary.csv",
        "results/summary_results.csv",
        "results/interference_delta.csv"
    )

    print("Interference delta calculated.")