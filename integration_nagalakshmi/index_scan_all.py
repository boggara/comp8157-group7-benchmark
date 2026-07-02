import csv
import time
from pathlib import Path


def simulate_index_scan(database, records):
    start = time.perf_counter()

    if database == "PostgreSQL":
        time.sleep(records / 1000000)
    elif database == "MongoDB":
        time.sleep(records / 1200000)
    elif database == "Cassandra":
        time.sleep(records / 900000)
    elif database == "Neo4j":
        time.sleep(records / 1100000)

    end = time.perf_counter()
    return round((end - start) * 1000, 3)


def main():
    databases = ["PostgreSQL", "MongoDB", "Cassandra", "Neo4j"]
    record_sizes = [10000, 50000, 107000]

    Path("results").mkdir(exist_ok=True)

    with open("results/index_scan_results.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "database",
            "records",
            "index_scan_latency_ms"
        ])

        writer.writeheader()

        for db in databases:
            for size in record_sizes:
                latency = simulate_index_scan(db, size)

                writer.writerow({
                    "database": db,
                    "records": size,
                    "index_scan_latency_ms": latency
                })

    print("Index scan results saved.")


if __name__ == "__main__":
    main()