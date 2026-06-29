import time
from pymongo import MongoClient
import random

client = MongoClient("mongodb://localhost:27017/")
db = client["olist"]

results = {}

# --- 1. Point Lookup: Find a single order by order_id ---
sample_order = db.orders.find_one()
order_id = sample_order["order_id"]

start = time.time()
for _ in range(100):
    db.orders.find_one({"order_id": order_id})
end = time.time()
results["point_lookup_order"] = round((end - start) / 100 * 1000, 4)
print(f"Point Lookup (order by ID): {results['point_lookup_order']} ms avg")

# --- 2. Point Lookup: Find customer by customer_id ---
sample_customer = db.customers.find_one()
customer_id = sample_customer["customer_id"]

start = time.time()
for _ in range(100):
    db.customers.find_one({"customer_id": customer_id})
end = time.time()
results["point_lookup_customer"] = round((end - start) / 100 * 1000, 4)
print(f"Point Lookup (customer by ID): {results['point_lookup_customer']} ms avg")

# --- 3. Insert: Insert a new order ---
new_order = {
    "order_id": "test_order_001",
    "customer_id": customer_id,
    "order_status": "created",
    "order_purchase_timestamp": "2026-06-29 00:00:00",
    "order_delivered_customer_date": None,
    "order_estimated_delivery_date": "2026-07-10 00:00:00"
}

start = time.time()
for i in range(100):
    new_order["order_id"] = f"test_order_{i}"
    db.orders.insert_one(new_order.copy())
end = time.time()
results["insert_order"] = round((end - start) / 100 * 1000, 4)
print(f"Insert (new order): {results['insert_order']} ms avg")

# --- 4. Update: Update order status ---
start = time.time()
for i in range(100):
    db.orders.update_one({"order_id": f"test_order_{i}"}, {"$set": {"order_status": "delivered"}})
end = time.time()
results["update_order_status"] = round((end - start) / 100 * 1000, 4)
print(f"Update (order status): {results['update_order_status']} ms avg")

# --- 5. Delete: Clean up test orders ---
db.orders.delete_many({"order_id": {"$regex": "^test_order_"}})
print("Test orders cleaned up.")

print("\n--- OLTP Results (ms avg over 100 runs) ---")
for k, v in results.items():
    print(f"  {k}: {v} ms")

client.close()