import pandas as pd
from py2neo import Graph

graph = Graph("bolt://localhost:7687", auth=("neo4j", "neo4jpass123"))

# Indexes first - speeds up MERGE matching a lot
graph.run("CREATE INDEX customer_id IF NOT EXISTS FOR (c:Customer) ON (c.customer_id)")
graph.run("CREATE INDEX product_id IF NOT EXISTS FOR (p:Product) ON (p.product_id)")
graph.run("CREATE INDEX seller_id IF NOT EXISTS FOR (s:Seller) ON (s.seller_id)")

print("Loading CSVs...")
customers = pd.read_csv("data/olist_customers_dataset.csv")
sellers = pd.read_csv("data/olist_sellers_dataset.csv")
products = pd.read_csv("data/olist_products_dataset.csv")
order_items = pd.read_csv("data/olist_order_items_dataset.csv")
orders = pd.read_csv("data/olist_orders_dataset.csv")
reviews = pd.read_csv("data/olist_order_reviews_dataset.csv")

# ---- TEST MODE: limit to a 10K-order subset ----
TEST_MODE = False
if TEST_MODE:
    orders = orders.head(10000)
    valid_order_ids = set(orders["order_id"])
    order_items = order_items[order_items["order_id"].isin(valid_order_ids)]
    reviews = reviews[reviews["order_id"].isin(valid_order_ids)]
    valid_customer_ids = set(orders["customer_id"])
    customers = customers[customers["customer_id"].isin(valid_customer_ids)]
    valid_product_ids = set(order_items["product_id"])
    products = products[products["product_id"].isin(valid_product_ids)]
    valid_seller_ids = set(order_items["seller_id"])
    sellers = sellers[sellers["seller_id"].isin(valid_seller_ids)]

def batch(df, size=1000):
    for i in range(0, len(df), size):
        yield df.iloc[i:i+size]

print(f"Loading {len(customers)} customers...")
for chunk in batch(customers):
    rows = chunk[["customer_id"]].to_dict("records")
    graph.run("UNWIND $rows AS row MERGE (c:Customer {customer_id: row.customer_id})", rows=rows)

print(f"Loading {len(products)} products...")
for chunk in batch(products):
    rows = chunk[["product_id", "product_category_name"]].to_dict("records")
    graph.run("""
        UNWIND $rows AS row
        MERGE (p:Product {product_id: row.product_id})
        SET p.category = row.product_category_name
    """, rows=rows)

print(f"Loading {len(sellers)} sellers...")
for chunk in batch(sellers):
    rows = chunk[["seller_id"]].to_dict("records")
    graph.run("UNWIND $rows AS row MERGE (s:Seller {seller_id: row.seller_id})", rows=rows)

print("Loading PURCHASED and SOLD relationships...")
merged = order_items.merge(orders[["order_id", "customer_id"]], on="order_id")
for chunk in batch(merged):
    rows = chunk[["customer_id", "product_id", "seller_id"]].to_dict("records")
    graph.run("""
        UNWIND $rows AS row
        MATCH (c:Customer {customer_id: row.customer_id})
        MATCH (p:Product {product_id: row.product_id})
        MATCH (s:Seller {seller_id: row.seller_id})
        MERGE (c)-[:PURCHASED]->(p)
        MERGE (s)-[:SOLD]->(p)
    """, rows=rows)

print("Loading REVIEWED relationships...")
reviews_merged = reviews.merge(orders[["order_id", "customer_id"]], on="order_id")
reviews_merged = reviews_merged.merge(order_items[["order_id", "product_id"]], on="order_id")
reviews_merged = reviews_merged.dropna(subset=["customer_id", "product_id", "review_score"])

for chunk in batch(reviews_merged):
    rows = chunk[["customer_id", "product_id", "review_score"]].to_dict("records")
    graph.run("""
        UNWIND $rows AS row
        MATCH (c:Customer {customer_id: row.customer_id})
        MATCH (p:Product {product_id: row.product_id})
        MERGE (c)-[r:REVIEWED]->(p)
        SET r.score = row.review_score
    """, rows=rows)

print("Ingestion complete.")