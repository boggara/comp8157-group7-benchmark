import csv
from pathlib import Path
import matplotlib.pyplot as plt


def read_csv(filename):
    with open(filename, "r") as f:
        return list(csv.DictReader(f))


def plot_latency():
    rows = read_csv("results/summary_results.csv")

    labels = []
    values = []

    for row in rows:
        labels.append(row["database"] + "-" + row["workload"])
        values.append(float(row["avg_ms"]))

    Path("graphs").mkdir(exist_ok=True)

    plt.figure(figsize=(12, 6))
    plt.bar(labels, values)
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Average Latency (ms)")
    plt.title("Average Latency by Database and Workload")
    plt.tight_layout()
    plt.savefig("graphs/latency_distribution.png")
    plt.close()


def plot_index_scan():
    rows = read_csv("results/index_scan_results.csv")

    databases = sorted(set(row["database"] for row in rows))

    Path("graphs").mkdir(exist_ok=True)

    for db in databases:
        x = []
        y = []

        for row in rows:
            if row["database"] == db:
                x.append(int(row["records"]))
                y.append(float(row["index_scan_latency_ms"]))

        plt.plot(x, y, marker="o", label=db)

    plt.xlabel("Number of Records")
    plt.ylabel("Index Scan Latency (ms)")
    plt.title("Index Scan Degradation")
    plt.legend()
    plt.tight_layout()
    plt.savefig("graphs/index_degradation.png")
    plt.close()


def main():
    plot_latency()
    plot_index_scan()
    print("Graphs created in graphs folder.")


if __name__ == "__main__":
    main()