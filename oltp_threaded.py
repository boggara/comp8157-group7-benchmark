import time
import threading
import numpy as np
from pymongo import MongoClient

def get_db():
    client = MongoClient("mongodb://localhost:27017/")
    return client, client["olist"]

def run_oltp(thread_count, operation, label):
    latencies = []
    lock = threading.Lock()

    def worker():
        client, db = get_db()
        for _ in range(10):
            start = time.time()
            operation(db)
            end = time.time()
            with lock:
                latencies.append((end - start) * 1000)
        client.close()

    threads = [threading.Thread(target=worker) for _ in range(thread_count)]
    for t in threads: t.start()
    for t in threads: t.join()

    p50 = round(np.percentile(latencies, 50), 4)
    p95 = round(np.percentile(latencies, 95), 4)
    p99 = round(np.percentile(latencies, 99), 4)
    print(f"  {label} | threads={thread_count} | p50={p50}ms p95={p95}ms p99={p99}ms")
    return {"threads": thread_count, "p50": p50, "p95": p95, "p99": p99}

# Get sample IDs
client, db = get_db()
sample_order = db.orders_embedded.find_one()
order_id = sample_order["order_id"]
client.close()

results = {}

# --- INSERT ---
print("INSERT operation:")
def insert_op(db):
    db.orders_embedded.insert_one({
        "order_id": f"test_{time.time()}",
        "order_status": "created",
        "items": [], "payments": [], "reviews": []
    })
    db.orders_embedded.delete_one({"order_status": "created", "items": []})

results["insert"] = []
for t in [1, 10, 50, 100]:
    results["insert"].append(run_oltp(t, insert_op, "INSERT"))

# --- UPDATE ---
print("UPDATE operation:")
def update_op(db):
    db.orders_embedded.update_one(
        {"order_id": order_id},
        {"$set": {"order_status": "delivered"}}
    )

results["update"] = []
for t in [1, 10, 50, 100]:
    results["update"].append(run_oltp(t, update_op, "UPDATE"))

# --- POINT LOOKUP ---
print("POINT LOOKUP operation:")
def lookup_op(db):
    db.orders_embedded.find_one({"order_id": order_id})

results["lookup"] = []
for t in [1, 10, 50, 100]:
    results["lookup"].append(run_oltp(t, lookup_op, "LOOKUP"))

import json
with open("C:/olist_project/oltp_threaded_results.json", "w") as f:
    json.dump(results, f, indent=2)
print("\nResults saved to oltp_threaded_results.json")