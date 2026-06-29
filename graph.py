import time
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["olist"]

results = {}

# --- $graphLookup: Find all orders and their items for a customer (order chain) ---
sample_customer = db.customers.find_one()
customer_id = sample_customer["customer_id"]

start = time.time()
pipeline = [
    {"$match": {"customer_id": customer_id}},
    {"$lookup": {
        "from": "order_items",
        "localField": "order_id",
        "foreignField": "order_id",
        "as": "items"
    }},
    {"$lookup": {
        "from": "order_payments",
        "localField": "order_id",
        "foreignField": "order_id",
        "as": "payments"
    }},
    {"$lookup": {
        "from": "order_reviews",
        "localField": "order_id",
        "foreignField": "order_id",
        "as": "reviews"
    }}
]
result = list(db.orders.aggregate(pipeline))
end = time.time()
results["order_chain_lookup"] = round((end - start) * 1000, 4)
print(f"Order chain lookup: {results['order_chain_lookup']} ms")
print(f"  -> Found {len(result)} order(s) for customer")

# --- $graphLookup: Seller network traversal ---
sample_seller = db.sellers.find_one()
seller_id = sample_seller["seller_id"]

start = time.time()
pipeline2 = [
    {"$match": {"seller_id": seller_id}},
    {"$graphLookup": {
        "from": "order_items",
        "startWith": "$seller_id",
        "connectFromField": "seller_id",
        "connectToField": "seller_id",
        "as": "seller_orders",
        "maxDepth": 2
    }},
    {"$project": {
        "seller_id": 1,
        "seller_city": 1,
        "total_orders": {"$size": "$seller_orders"}
    }}
]
result2 = list(db.sellers.aggregate(pipeline2))
end = time.time()
results["graphlookup_seller_network"] = round((end - start) * 1000, 4)
print(f"graphLookup seller network: {results['graphlookup_seller_network']} ms")
if result2:
    print(f"  -> Seller has {result2[0].get('total_orders', 0)} order items")

print("\n--- Graph Query Results (ms, single run) ---")
for k, v in results.items():
    print(f"  {k}: {v} ms")

client.close()