import time
import json
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["olist"]
results = {}

# --- 1. Revenue by product category ---
start = time.time()
pipeline1 = [
    {"$unwind": "$items"},
    {"$lookup": {"from": "products", "localField": "items.product_id", "foreignField": "product_id", "as": "product"}},
    {"$unwind": {"path": "$product", "preserveNullAndEmptyArrays": True}},
    {"$group": {"_id": "$product.product_category_name", "total_revenue": {"$sum": "$items.price"}}},
    {"$sort": {"total_revenue": -1}},
    {"$limit": 10}
]
list(db.orders_embedded.aggregate(pipeline1, allowDiskUse=True))
end = time.time()
results["revenue_by_category"] = round((end - start) * 1000, 4)
print(f"Revenue by category: {results['revenue_by_category']} ms")

# --- 2. Avg delivery time by seller region ---
start = time.time()
pipeline2 = [
    {"$match": {
        "order_delivered_customer_date": {"$ne": None, "$exists": True},
        "order_purchase_timestamp": {"$ne": None, "$exists": True}
    }},
    {"$unwind": "$items"},
    {"$lookup": {"from": "sellers", "localField": "items.seller_id", "foreignField": "seller_id", "as": "seller"}},
    {"$unwind": {"path": "$seller", "preserveNullAndEmptyArrays": True}},
    {"$addFields": {
        "purchase_date": {
            "$dateFromString": {
                "dateString": {"$substr": ["$order_purchase_timestamp", 0, 19]},
                "format": "%Y-%m-%d %H:%M:%S",
                "onError": None,
                "onNull": None
            }
        },
        "delivered_date": {
            "$dateFromString": {
                "dateString": {"$substr": ["$order_delivered_customer_date", 0, 19]},
                "format": "%Y-%m-%d %H:%M:%S",
                "onError": None,
                "onNull": None
            }
        }
    }},
    {"$match": {"purchase_date": {"$ne": None}, "delivered_date": {"$ne": None}}},
    {"$project": {
        "seller_state": "$seller.seller_state",
        "delivery_days": {"$divide": [{"$subtract": ["$delivered_date", "$purchase_date"]}, 86400000]}
    }},
    {"$group": {"_id": "$seller_state", "avg_delivery_days": {"$avg": "$delivery_days"}}},
    {"$sort": {"avg_delivery_days": 1}},
    {"$limit": 10}
]
list(db.orders_embedded.aggregate(pipeline2, allowDiskUse=True))
end = time.time()
results["avg_delivery_by_seller_region"] = round((end - start) * 1000, 4)
print(f"Avg delivery time by seller region: {results['avg_delivery_by_seller_region']} ms")

# --- 3. Top customers by lifetime value ---
start = time.time()
pipeline3 = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$customer_id",
        "lifetime_value": {"$sum": "$items.price"},
        "total_orders": {"$sum": 1}
    }},
    {"$sort": {"lifetime_value": -1}},
    {"$limit": 10}
]
list(db.orders_embedded.aggregate(pipeline3, allowDiskUse=True))
end = time.time()
results["top_customers_by_lifetime_value"] = round((end - start) * 1000, 4)
print(f"Top customers by lifetime value: {results['top_customers_by_lifetime_value']} ms")

# --- 4. Freight cost distribution over time ---
start = time.time()
pipeline4 = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": {"$substr": ["$order_purchase_timestamp", 0, 7]},
        "avg_freight": {"$avg": "$items.freight_value"},
        "total_freight": {"$sum": "$items.freight_value"},
        "order_count": {"$sum": 1}
    }},
    {"$sort": {"_id": 1}}
]
list(db.orders_embedded.aggregate(pipeline4, allowDiskUse=True))
end = time.time()
results["freight_cost_distribution"] = round((end - start) * 1000, 4)
print(f"Freight cost distribution over time: {results['freight_cost_distribution']} ms")

with open("C:/olist_project/olap_correct_results.json", "w") as f:
    json.dump(results, f, indent=2)
print("\nResults saved to olap_correct_results.json")
client.close()