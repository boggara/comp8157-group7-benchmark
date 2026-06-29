from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["olist"]

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