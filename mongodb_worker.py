import time
import json
import threading
import numpy as np
from pymongo import MongoClient

def get_db():
    client = MongoClient("mongodb://localhost:27017/")
    return client, client["olist"]

def run_workload(thread_count, duration_seconds=30):
    """
    Runs all 3 workload types simultaneously for a given duration.
    This is called by the group integration harness.
    """
    results = {"oltp": [], "olap": [], "graph": []}
    stop_event = threading.Event()
    lock = threading.Lock()

    # --- OLTP worker ---
    def oltp_worker():
        client, db = get_db()
        sample = db.orders_embedded.find_one()
        order_id = sample["order_id"]
        while not stop_event.is_set():
            start = time.time()
            db.orders_embedded.update_one(
                {"order_id": order_id},
                {"$set": {"order_status": "delivered"}}
            )
            end = time.time()
            with lock:
                results["oltp"].append((end - start) * 1000)
        client.close()

    # --- OLAP worker ---
    def olap_worker():
        client, db = get_db()
        while not stop_event.is_set():
            start = time.time()
            list(db.orders_embedded.aggregate([
                {"$unwind": "$items"},
                {"$group": {"_id": "$customer_id", "total": {"$sum": "$items.price"}}},
                {"$sort": {"total": -1}},
                {"$limit": 10}
            ], allowDiskUse=True))
            end = time.time()
            with lock:
                results["olap"].append((end - start) * 1000)
        client.close()

    # --- Graph worker ---
    def graph_worker():
        client, db = get_db()
        sample = db.orders_embedded.find_one({"items": {"$ne": []}})
        customer_id = sample["customer_id"]
        while not stop_event.is_set():
            start = time.time()
            list(db.orders_embedded.aggregate([
                {"$match": {"customer_id": customer_id}},
                {"$unwind": "$items"},
                {"$group": {"_id": "$customer_id", "products": {"$addToSet": "$items.product_id"}}},
                {"$graphLookup": {
                    "from": "orders_embedded",
                    "startWith": "$products",
                    "connectFromField": "products",
                    "connectToField": "items.product_id",
                    "as": "similar_customers",
                    "maxDepth": 1
                }},
                {"$project": {"count": {"$size": "$similar_customers"}}}
            ], allowDiskUse=True))
            end = time.time()
            with lock:
                results["graph"].append((end - start) * 1000)
        client.close()

    # Launch all workers
    threads = []
    for _ in range(thread_count):
        threads.append(threading.Thread(target=oltp_worker))
    threads.append(threading.Thread(target=olap_worker))
    threads.append(threading.Thread(target=graph_worker))

    for t in threads:
        t.start()

    time.sleep(duration_seconds)
    stop_event.set()

    for t in threads:
        t.join()

    # Calculate metrics
    def metrics(data):
        if not data:
            return {"p50": 0, "p95": 0, "p99": 0, "count": 0}
        return {
            "p50": round(np.percentile(data, 50), 4),
            "p95": round(np.percentile(data, 95), 4),
            "p99": round(np.percentile(data, 99), 4),
            "count": len(data)
        }

    return {
        "threads": thread_count,
        "duration_seconds": duration_seconds,
        "oltp": metrics(results["oltp"]),
        "olap": metrics(results["olap"]),
        "graph": metrics(results["graph"])
    }

if __name__ == "__main__":
    print("Running MongoDB co-scheduled workload (30 seconds)...")
    for t in [1, 10, 50, 100]:
        print(f"\nThread count: {t}")
        result = run_workload(thread_count=t, duration_seconds=30)
        print(f"  OLTP  -> p50={result['oltp']['p50']}ms p95={result['oltp']['p95']}ms p99={result['oltp']['p99']}ms ops={result['oltp']['count']}")
        print(f"  OLAP  -> p50={result['olap']['p50']}ms p95={result['olap']['p95']}ms p99={result['olap']['p99']}ms ops={result['olap']['count']}")
        print(f"  Graph -> p50={result['graph']['p50']}ms p95={result['graph']['p95']}ms p99={result['graph']['p99']}ms ops={result['graph']['count']}")

   print("\nDone. The integration harness can import run_workload() from this script.")