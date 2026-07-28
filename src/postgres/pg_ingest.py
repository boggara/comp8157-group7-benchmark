"""
COMP 8157 Group 7 - PostgreSQL ingestion (Olist dataset)
Sai Srinivas Uppara - PostgreSQL pipeline

Loads the nine Olist CSVs into PostgreSQL with FK-safe ordering
(parents before children) using COPY for speed. Client encoding is
forced to UTF-8 because the Olist data contains Portuguese text.

TEST_MODE=1 loads only a 10K-order slice (plus the rows the slice
references) to verify the pipeline end to end before a full load.

Usage:
    psql -h localhost -U bench -d olist -f pg_schema.sql
    python pg_ingest.py                 # full 107K-order load
    TEST_MODE=1 python pg_ingest.py     # 10K-order test slice
"""

import csv
import io
import os
import sys
import time

import pandas as pd
import psycopg2

PG_DSN = os.environ.get(
    "PG_DSN", "host=localhost port=5432 dbname=olist user=bench password=benchpass123"
)
DATA_PATH = os.environ.get("OLIST_DATA", "data")
TEST_MODE = os.environ.get("TEST_MODE", "0") == "1"
TEST_ORDERS = int(os.environ.get("TEST_ORDERS", "10000"))

FILES = {
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
}

# FK-safe load order: parents first, then orders, then order children.
LOAD_ORDER = [
    "customers", "sellers", "products", "category_translation",
    "geolocation", "orders", "order_items", "order_payments", "order_reviews",
]


def read_csv(name):
    df = pd.read_csv(os.path.join(DATA_PATH, FILES[name]), dtype=str, keep_default_na=False)
    df = df.replace({"": None})
    return df


def copy_df(cur, table, df, columns=None):
    """Stream a dataframe into a table with COPY (fast bulk load)."""
    if columns is None:
        columns = list(df.columns)
    buf = io.StringIO()
    writer = csv.writer(buf)
    for row in df[columns].itertuples(index=False, name=None):
        writer.writerow(["\\N" if v is None else v for v in row])
    buf.seek(0)
    cols = ", ".join(columns)
    cur.copy_expert(
        f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf
    )


def main():
    conn = psycopg2.connect(PG_DSN)
    conn.set_client_encoding("UTF8")  # Olist text is Portuguese; avoids encoding errors
    cur = conn.cursor()

    print(f"Loading Olist into PostgreSQL ({'TEST slice' if TEST_MODE else 'FULL dataset'})")

    # Make the load re-runnable: clear all tables (children first via CASCADE).
    cur.execute("TRUNCATE " + ", ".join(LOAD_ORDER) + " CASCADE")
    conn.commit()

    dfs = {name: read_csv(name) for name in LOAD_ORDER}

    if TEST_MODE:
        # Take the first N orders, then restrict every child/parent table to
        # exactly the rows that slice references so all FKs still hold.
        orders = dfs["orders"].head(TEST_ORDERS)
        order_ids = set(orders["order_id"])
        dfs["orders"] = orders
        dfs["order_items"] = dfs["order_items"][dfs["order_items"]["order_id"].isin(order_ids)]
        dfs["order_payments"] = dfs["order_payments"][dfs["order_payments"]["order_id"].isin(order_ids)]
        dfs["order_reviews"] = dfs["order_reviews"][dfs["order_reviews"]["order_id"].isin(order_ids)]
        dfs["customers"] = dfs["customers"][dfs["customers"]["customer_id"].isin(set(orders["customer_id"]))]
        dfs["products"] = dfs["products"][dfs["products"]["product_id"].isin(set(dfs["order_items"]["product_id"]))]
        dfs["sellers"] = dfs["sellers"][dfs["sellers"]["seller_id"].isin(set(dfs["order_items"]["seller_id"]))]

    # Raw Olist data quirks that would otherwise violate constraints:
    dfs["order_reviews"] = dfs["order_reviews"].drop_duplicates(subset=["review_id", "order_id"])
    dfs["category_translation"] = dfs["category_translation"].drop_duplicates(
        subset=["product_category_name"]
    )

    total_start = time.time()
    for name in LOAD_ORDER:
        df = dfs[name]
        start = time.time()
        copy_df(cur, name, df)
        conn.commit()
        print(f"  {name:22s} {len(df):>7d} rows in {time.time() - start:6.2f}s")

    # Validate row counts against what we intended to load.
    print("\nValidation (table count vs source count):")
    ok = True
    for name in LOAD_ORDER:
        cur.execute(f"SELECT count(*) FROM {name}")
        db_count = cur.fetchone()[0]
        match = "OK" if db_count == len(dfs[name]) else "MISMATCH"
        ok = ok and db_count == len(dfs[name])
        print(f"  {name:22s} db={db_count:>7d} src={len(dfs[name]):>7d}  {match}")

    # Spot-check FK integrity: orphaned order_items would indicate a bad load.
    cur.execute("""
        SELECT count(*) FROM order_items oi
        LEFT JOIN orders o ON o.order_id = oi.order_id
        WHERE o.order_id IS NULL
    """)
    orphans = cur.fetchone()[0]
    print(f"  orphaned order_items: {orphans}")

    # Refresh planner statistics before any timed run.
    conn.commit()  # close the validation transaction before autocommit VACUUM
    conn.autocommit = True
    cur.execute("VACUUM ANALYZE")
    print(f"\nVACUUM ANALYZE done. Total load time {time.time() - total_start:.1f}s")

    cur.close()
    conn.close()
    sys.exit(0 if ok and orphans == 0 else 1)


if __name__ == "__main__":
    main()
