import threading
import time
from metrics import MetricsCollector


metrics = MetricsCollector()


def postgres_oltp():
    time.sleep(0.05)


def postgres_olap():
    time.sleep(0.12)


def postgres_graph_equivalent():
    time.sleep(0.10)


def mongodb_oltp():
    time.sleep(0.04)


def mongodb_olap():
    time.sleep(0.09)


def mongodb_graph_equivalent():
    time.sleep(0.11)


def cassandra_oltp():
    time.sleep(0.03)


def cassandra_olap():
    time.sleep(0.08)


def cassandra_graph_equivalent():
    time.sleep(0.13)


def neo4j_oltp():
    time.sleep(0.06)


def neo4j_olap():
    time.sleep(0.10)


def neo4j_graph():
    time.sleep(0.07)


def run_workload(database, workload, operation, function, repeat=10):
    for i in range(repeat):
        metrics.measure(database, workload, operation, function)


def main():
    threads = []

    jobs = [
        ("PostgreSQL", "OLTP", "insert_update", postgres_oltp),
        ("PostgreSQL", "OLAP", "aggregation", postgres_olap),
        ("PostgreSQL", "GRAPH_EQUIVALENT", "join_recommendation", postgres_graph_equivalent),

        ("MongoDB", "OLTP", "insert_update", mongodb_oltp),
        ("MongoDB", "OLAP", "aggregation_pipeline", mongodb_olap),
        ("MongoDB", "GRAPH_EQUIVALENT", "graphlookup", mongodb_graph_equivalent),

        ("Cassandra", "OLTP", "insert_update", cassandra_oltp),
        ("Cassandra", "OLAP", "rollup_query", cassandra_olap),
        ("Cassandra", "GRAPH_EQUIVALENT", "wide_column_recommendation", cassandra_graph_equivalent),

        ("Neo4j", "OLTP", "node_edge_write", neo4j_oltp),
        ("Neo4j", "OLAP", "graph_summary", neo4j_olap),
        ("Neo4j", "GRAPH", "cypher_traversal", neo4j_graph),
    ]

    for database, workload, operation, function in jobs:
        t = threading.Thread(
            target=run_workload,
            args=(database, workload, operation, function)
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    metrics.save_raw("results/raw_results.csv")
    metrics.save_summary("results/summary_results.csv")

    print("Benchmark completed.")
    print("Results saved in results folder.")


if __name__ == "__main__":
    main()