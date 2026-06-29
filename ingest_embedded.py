import pandas as pd
from pymongo import MongoClient
import time

client = MongoClient("mongodb://localhost:27017/")
db = client["olist"]

data_path = "C:/olist/"

print("Loading CSV files...")
orders = pd.read_csv(data_path + "olist_orders_dataset.csv")
order_items = pd.read_csv(data_path + "olist_order_items_dataset.csv")
order_payments = pd.read_csv(data_path + "olist_order_payments_dataset.csv")
order_reviews = pd.read_csv(data_path + "olist_order_reviews_dataset.csv")
customers = pd.read_csv(data_path + "olist_customers_dataset.csv")
products = pd.read_csv(data_path + "olist_products_dataset.csv")
sellers = pd.read_csv(data_path + "olist_sellers_dataset.csv")

# Convert to dicts grouped by order_id
print("Grouping by order_id...")
items_by_order = order_items.groupby("order_id").apply(lambda x: x.to_dict("records")).to_dict()
payments_by_order = order_payments.groupby("order_id").apply(lambda x: x.to_dict("records")).to_dict()
reviews_by_order = order_reviews.groupby("order_id").apply(lambda x: x.to_dict("records")).to_dict()
customers_by_id = customers.set_index("customer_id").to_dict("index")

print("Building embedded documents...")
embedded_orders = []
for _, row in orders.iterrows():
    oid = row["order_id"]
    cid = row["customer_id"]
    doc = row.to_dict()
    doc["items"] = items_by_order.get(oid, [])
    doc["payments"] = payments_by_order.get(oid, [])
    doc["reviews"] = reviews_by_order.get(oid, [])
    doc["customer"] = customers_by_id.get(cid, {})
    embedded_orders.append(doc)

print(f"Inserting {len(embedded_orders)} embedded order documents...")
db["orders_embedded"].drop()
# Insert in batches of 1000
batch_size = 1000
for i in range(0, len(embedded_orders), batch_size):
    db["orders_embedded"].insert_many(embedded_orders[i:i+batch_size])
    print(f"  Inserted {min(i+batch_size, len(embedded_orders))}/{len(embedded_orders)}")

# Keep flat collections too for reference
print("\nEmbedded ingestion complete.")
print(f"Total documents in orders_embedded: {db['orders_embedded'].count_documents({})}")
client.close()