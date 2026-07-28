import time
import csv
import statistics
from pathlib import Path


class MetricsCollector:
    def __init__(self):
        self.records = []

    def measure(self, database, workload, operation, func):
        start = time.perf_counter()
        success = True
        error = ""

        try:
            func()
        except Exception as e:
            success = False
            error = str(e)

        end = time.perf_counter()
        latency_ms = (end - start) * 1000

        self.records.append({
            "database": database,
            "workload": workload,
            "operation": operation,
            "latency_ms": round(latency_ms, 3),
            "success": success,
            "error": error
        })

    def summary(self):
        result = {}

        for r in self.records:
            key = (r["database"], r["workload"])
            result.setdefault(key, [])
            if r["success"]:
                result[key].append(r["latency_ms"])

        rows = []

        for (database, workload), values in result.items():
            if not values:
                continue

            values.sort()
            rows.append({
                "database": database,
                "workload": workload,
                "count": len(values),
                "avg_ms": round(statistics.mean(values), 3),
                "p50_ms": round(values[int(len(values) * 0.50)], 3),
                "p95_ms": round(values[int(len(values) * 0.95) - 1], 3),
                "p99_ms": round(values[int(len(values) * 0.99) - 1], 3)
            })

        return rows

    def save_raw(self, filename):
        Path("results").mkdir(exist_ok=True)

        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "database", "workload", "operation",
                "latency_ms", "success", "error"
            ])
            writer.writeheader()
            writer.writerows(self.records)

    def save_summary(self, filename):
        Path("results").mkdir(exist_ok=True)

        rows = self.summary()

        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "database", "workload", "count",
                "avg_ms", "p50_ms", "p95_ms", "p99_ms"
            ])
            writer.writeheader()
            writer.writerows(rows)