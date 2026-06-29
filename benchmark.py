import time
import json
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["olist"]

RUNS = 5
results = {}

def avg_time(fn, runs=RUNS):
    times = []
    for _ in range(runs):
        start = time.time()
        fn()
        end = time.time()
        times.append((end - start) * 1000)
    return round(sum(times) / len(times), 4)

# --- OLTP ---
sample_order = db.orders.find_one()
order_id = sample_order["order_id"]
sample_customer = db.customers.find_one()
customer_id = sample_customer["customer_id"]

results["oltp_point_lookup_order"] = avg_time(
    lambda: db.orders.find_one({"order_id": order_id})
)
results["oltp_point_lookup_customer"] = avg_time(
    lambda: db.customers.find_one({"customer_id": customer_id})
)

def insert_delete():
    db.orders.insert_one({"order_id": "bench_test", "customer_id": customer_id, "order_status": "created"})
    db.orders.delete_one({"order_id": "bench_test"})

results["oltp_insert_delete_order"] = avg_time(insert_delete)

results["oltp_update_order_status"] = avg_time(
    lambda: db.orders.update_one({"order_id": order_id}, {"$set": {"order_status": "delivered"}})
)

# --- OLAP ---
pipeline_revenue = [
    {"$lookup": {"from": "products", "localField": "product_id", "foreignField": "product_id", "as": "product"}},
    {"$unwind": "$product"},
    {"$group": {"_id": "$product.product_category_name", "total_revenue": {"$sum": "$price"}}},
    {"$sort": {"total_revenue": -1}},
    {"$limit": 10}
]
results["olap_revenue_by_category"] = avg_time(
    lambda: list(db.order_items.aggregate(pipeline_revenue))
)

pipeline_monthly = [
    {"$group": {"_id": {"$substr": ["$order_purchase_timestamp", 0, 7]}, "count": {"$sum": 1}}},
    {"$sort": {"_id": 1}}
]
results["olap_monthly_order_volume"] = avg_time(
    lambda: list(db.orders.aggregate(pipeline_monthly))
)

pipeline_cities = [
    {"$group": {"_id": "$customer_city", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
]
results["olap_top_cities"] = avg_time(
    lambda: list(db.customers.aggregate(pipeline_cities))
)

# --- Graph ---
sample_seller = db.sellers.find_one()
seller_id = sample_seller["seller_id"]

pipeline_graph = [
    {"$match": {"seller_id": seller_id}},
    {"$graphLookup": {
        "from": "order_items",
        "startWith": "$seller_id",
        "connectFromField": "seller_id",
        "connectToField": "seller_id",
        "as": "seller_orders",
        "maxDepth": 2
    }},
    {"$project": {"seller_id": 1, "total_orders": {"$size": "$seller_orders"}}}
]
results["graph_seller_network"] = avg_time(
    lambda: list(db.sellers.aggregate(pipeline_graph))
)

# --- Print and Save ---
print(f"\n--- MongoDB Benchmark Results (avg over {RUNS} runs) ---")
for k, v in results.items():
    print(f"  {k}: {v} ms")

with open("C:/olist_project/benchmark_results.json", "w") as f:
    json.dump(results, f, indent=2)

print("\nResults saved to C:/olist_project/benchmark_results.json")
client.close()