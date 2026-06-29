import pandas as pd
from pymongo import MongoClient
import time

client = MongoClient("mongodb://localhost:27017/")
db = client["olist"]

data_path = "C:/olist/"

files = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

for collection_name, filename in files.items():
    print(f"Loading {filename}...")
    df = pd.read_csv(data_path + filename)
    records = df.to_dict("records")
    db[collection_name].drop()
    db[collection_name].insert_many(records)
    print(f"  -> Inserted {len(records)} documents into '{collection_name}'")

print("\nIngestion complete.")
client.close()