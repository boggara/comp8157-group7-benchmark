import time
import json
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["olist"]
results = {}

# Get a sample customer
sample = db.orders_embedded.find_one({"items": {"$ne": []}})
customer_id = sample["customer_id"]

# --- $graphLookup: Find customers within 2 hops who share purchase history ---
start = time.time()
pipeline = [
    {"$match": {"customer_id": customer_id}},
    {"$unwind": "$items"},
    {"$group": {"_id": "$customer_id", "products": {"$addToSet": "$items.product_id"}}},
    {"$graphLookup": {
        "from": "orders_embedded",
        "startWith": "$products",
        "connectFromField": "products",
        "connectToField": "items.product_id",
        "as": "similar_customers",
        "maxDepth": 2,
        "restrictSearchWithMatch": {"customer_id": {"$ne": customer_id}}
    }},
    {"$project": {
        "customer_id": "$_id",
        "similar_customer_count": {"$size": "$similar_customers"}
    }}
]
result = list(db.orders_embedded.aggregate(pipeline, allowDiskUse=True))
end = time.time()
results["graphlookup_copurchase"] = round((end - start) * 1000, 4)
print(f"$graphLookup co-purchase: {results['graphlookup_copurchase']} ms")
if result:
    print(f"  -> Found {result[0].get('similar_customer_count', 0)} similar customers within 2 hops")

with open("C:/olist_project/graph_copurchase_results.json", "w") as f:
    json.dump(results, f, indent=2)
print("Results saved to graph_copurchase_results.json")
client.close()