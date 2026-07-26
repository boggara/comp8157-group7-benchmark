from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["olist"]

# Indexes for the embedded-document collection actually used by the live
# benchmark harness (mongodb_worker.py, mongodb_baseline.py, scaling.py all
# query orders_embedded). These were previously missing - the indexes below
# used to target a separate, unpopulated flat "orders" collection, which
# meant every OLTP point-lookup and update against orders_embedded was doing
# a full collection scan instead of an index lookup.
db.orders_embedded.create_index("order_id")
db.orders_embedded.create_index("customer_id")

# Indexes for the alternate flat-collection loader (ingest.py), kept for
# completeness since that ingestion path is documented separately in
# README.md and may still be populated for the flat-vs-embedded comparison
# referenced in D.3.1.
db.orders.create_index("order_id")
db.orders.create_index("customer_id")
db.order_items.create_index("order_id")
db.order_items.create_index("product_id")
db.order_items.create_index("seller_id")
db.order_reviews.create_index("order_id")
db.customers.create_index("customer_id")
db.products.create_index("product_id")
db.sellers.create_index("seller_id")

print("All indexes created.")
client.close()