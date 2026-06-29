import time
import json
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["olist"]
results = {}

# Get all order IDs
all_orders = list(db.orders_embedded.find({}, {"order_id": 1, "_id": 0}))
total = len(all_orders)
print(f"Total orders: {total}")

subsets = {"10K": 10000, "50K": 50000, "107K": total}

for label, size in subsets.items():
    subset_ids = [o["order_id"] for o in all_orders[:size]]
    
    # Create temp collection
    db["orders_scale_temp"].drop()
    docs = list(db.orders_embedded.find({"order_id": {"$in": subset_ids}}))
    db["orders_scale_temp"].insert_many(docs)
    db["orders_scale_temp"].create_index("order_id")
    db["orders_scale_temp"].create_index("customer_id")

    # Point lookup
    test_id = subset_ids[size // 2]
    times = []
    for _ in range(20):
        start = time.time()
        db["orders_scale_temp"].find_one({"order_id": test_id})
        end = time.time()
        times.append((end - start) * 1000)
    lookup_avg = round(sum(times) / len(times), 4)

    # Aggregation
    start = time.time()
    list(db["orders_scale_temp"].aggregate([
        {"$unwind": "$items"},
        {"$group": {"_id": "$customer_id", "total": {"$sum": "$items.price"}}},
        {"$sort": {"total": -1}},
        {"$limit": 10}
    ], allowDiskUse=True))
    end = time.time()
    agg_time = round((end - start) * 1000, 4)

    results[label] = {"point_lookup_ms": lookup_avg, "aggregation_ms": agg_time}
    print(f"{label}: point_lookup={lookup_avg}ms | aggregation={agg_time}ms")

db["orders_scale_temp"].drop()

with open("C:/olist_project/scaling_results.json", "w") as f:
    json.dump(results, f, indent=2)
print("\nResults saved to scaling_results.json")
client.close()