import time
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["olist"]

results = {}

# --- 1. Total revenue per product category ---
start = time.time()
pipeline1 = [
    {"$lookup": {
        "from": "products",
        "localField": "product_id",
        "foreignField": "product_id",
        "as": "product"
    }},
    {"$unwind": "$product"},
    {"$group": {
        "_id": "$product.product_category_name",
        "total_revenue": {"$sum": "$price"},
        "total_orders": {"$sum": 1}
    }},
    {"$sort": {"total_revenue": -1}},
    {"$limit": 10}
]
list(db.order_items.aggregate(pipeline1))
end = time.time()
results["revenue_by_category"] = round((end - start) * 1000, 4)
print(f"Revenue by category: {results['revenue_by_category']} ms")

# --- 2. Monthly order volume ---
start = time.time()
pipeline2 = [
    {"$group": {
        "_id": {"$substr": ["$order_purchase_timestamp", 0, 7]},
        "order_count": {"$sum": 1}
    }},
    {"$sort": {"_id": 1}}
]
list(db.orders.aggregate(pipeline2))
end = time.time()
results["monthly_order_volume"] = round((end - start) * 1000, 4)
print(f"Monthly order volume: {results['monthly_order_volume']} ms")

# --- 3. Average review score per seller ---
start = time.time()
pipeline3 = [
    {"$lookup": {
        "from": "order_reviews",
        "localField": "order_id",
        "foreignField": "order_id",
        "as": "review"
    }},
    {"$unwind": "$review"},
    {"$group": {
        "_id": "$seller_id",
        "avg_score": {"$avg": "$review.review_score"},
        "total_reviews": {"$sum": 1}
    }},
    {"$sort": {"avg_score": -1}},
    {"$limit": 10}
]
list(db.order_items.aggregate(pipeline3))
end = time.time()
results["avg_review_per_seller"] = round((end - start) * 1000, 4)
print(f"Avg review score per seller: {results['avg_review_per_seller']} ms")

# --- 4. Top 10 cities by number of customers ---
start = time.time()
pipeline4 = [
    {"$group": {
        "_id": "$customer_city",
        "customer_count": {"$sum": 1}
    }},
    {"$sort": {"customer_count": -1}},
    {"$limit": 10}
]
list(db.customers.aggregate(pipeline4))
end = time.time()
results["top_cities_by_customers"] = round((end - start) * 1000, 4)
print(f"Top cities by customers: {results['top_cities_by_customers']} ms")

print("\n--- OLAP Results (ms, single run) ---")
for k, v in results.items():
    print(f"  {k}: {v} ms")

client.close()