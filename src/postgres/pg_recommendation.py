"""
COMP 8157 Group 7 - PostgreSQL recommendation-equivalent queries
Sai Srinivas Uppara - PostgreSQL pipeline

Join/subquery versions of the three graph traversal queries in
neo4j_queries.cypher, so the same logical question the graph team
answers in Neo4j is posed to PostgreSQL in its native form:

  1. Multi-hop co-purchase recommendation
     (products bought by customers who share purchase history with a target)
  2. Seller network (sellers linked through shared products)
  3. Customer similarity within two hops (overlapping purchase histories)

A "purchase" edge = customer -> order -> order_item -> product,
which mirrors the (:Customer)-[:PURCHASED]->(:Product) edge Neo4j uses.
"""

import json
import os
import time

import psycopg2

PG_DSN = os.environ.get(
    "PG_DSN", "host=localhost port=5432 dbname=olist user=bench password=benchpass123"
)
RUNS = int(os.environ.get("RUNS", "3"))

# Materialize the purchase edge once as a CTE used by all three queries.
PURCHASES_CTE = """
    WITH purchases AS (
        SELECT o.customer_id, oi.product_id
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
    )
"""

QUERIES = {
    # Cypher: (target)-[:PURCHASED]->(:Product)<-[:PURCHASED]-(other)-[:PURCHASED]->(rec)
    #         WHERE NOT (target)-[:PURCHASED]->(rec)
    "copurchase_recommendation": PURCHASES_CTE + """
        SELECT rec.product_id, COUNT(*) AS strength
        FROM purchases target
        JOIN purchases other
             ON other.product_id = target.product_id
            AND other.customer_id <> target.customer_id
        JOIN purchases rec
             ON rec.customer_id = other.customer_id
        WHERE target.customer_id = %(customer_id)s
          AND rec.product_id NOT IN (
                SELECT product_id FROM purchases WHERE customer_id = %(customer_id)s
          )
        GROUP BY rec.product_id
        ORDER BY strength DESC
        LIMIT 10
    """,
    # Cypher: (s1:Seller)-[:SOLD]->(p1:Product)<-[:SOLD]-(s2:Seller)
    "seller_network": """
        SELECT a.seller_id AS seller_1, b.seller_id AS seller_2,
               COUNT(DISTINCT a.product_id) AS shared
        FROM (SELECT DISTINCT seller_id, product_id FROM order_items) a
        JOIN (SELECT DISTINCT seller_id, product_id FROM order_items) b
             ON a.product_id = b.product_id AND a.seller_id < b.seller_id
        GROUP BY a.seller_id, b.seller_id
        ORDER BY shared DESC
        LIMIT 10
    """,
    # Cypher: (c1)-[:PURCHASED]->(:Product)<-[:PURCHASED]-(c2)
    "customer_similarity": PURCHASES_CTE + """
        SELECT p2.customer_id, COUNT(*) AS overlap
        FROM purchases p1
        JOIN purchases p2
             ON p2.product_id = p1.product_id
            AND p2.customer_id <> p1.customer_id
        WHERE p1.customer_id = %(customer_id)s
        GROUP BY p2.customer_id
        ORDER BY overlap DESC
        LIMIT 10
    """,
}


def pick_target_customer(cur):
    """Pick a customer who actually shares products with other customers,
    so the traversal has work to do (same idea as the Mongo/Neo4j scripts)."""
    cur.execute("""
        SELECT o.customer_id
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        GROUP BY o.customer_id, oi.product_id
        HAVING COUNT(*) >= 1
        ORDER BY (
            SELECT COUNT(*) FROM order_items oi2
            WHERE oi2.product_id = MIN(oi.product_id)
        ) DESC
        LIMIT 1
    """)
    return cur.fetchone()[0]


def main():
    conn = psycopg2.connect(PG_DSN)
    conn.set_client_encoding("UTF8")
    cur = conn.cursor()
    customer_id = pick_target_customer(cur)
    print(f"Target customer: {customer_id}")

    results = {}
    for name, sql in QUERIES.items():
        params = {"customer_id": customer_id}
        cur.execute(sql, params)  # warm-up (not timed)
        cur.fetchall()
        times = []
        rows = None
        for _ in range(RUNS):
            start = time.time()
            cur.execute(sql, params)
            rows = cur.fetchall()
            times.append((time.time() - start) * 1000)
        median_ms = round(sorted(times)[len(times) // 2], 4)
        results[name] = median_ms
        print(f"{name}: {median_ms} ms (median of {RUNS}, {len(rows)} rows)")
        for r in rows[:3]:
            print(f"   {r}")

    out = os.environ.get("RESULTS_OUT", "pg_recommendation_results.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out}")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
