"""
cassandra_ingest.py
Owner: Alyan Khowaja (Cassandra role, Group 7)

Loads the Olist Brazilian E-Commerce dataset (Kaggle) into the
`olist_benchmark` keyspace. Joins orders + customers + order_items +
order_payments in pandas, then bulk-loads rows into Cassandra using
prepared statements with execute_concurrent_with_args() for throughput.

Usage:
    python cassandra_ingest.py --data-dir ./olist_csv --limit 10000
    python cassandra_ingest.py --data-dir ./olist_csv           # full 107K load

Expected input files (standard Kaggle Olist filenames):
    olist_orders_dataset.csv
    olist_customers_dataset.csv
    olist_order_items_dataset.csv
    olist_order_payments_dataset.csv
    olist_products_dataset.csv
    olist_product_category_name_translation.csv
    olist_sellers_dataset.csv
"""

import argparse
import sys
import time
from decimal import Decimal

import pandas as pd
from cassandra.cluster import Cluster
from cassandra.io.asyncioreactor import AsyncioConnection
Cluster.connection_class = AsyncioConnection
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import BatchStatement, ConsistencyLevel

CASSANDRA_HOSTS = ["127.0.0.1"]
KEYSPACE = "olist_benchmark"
CONCURRENCY = 100          # in-flight requests for execute_concurrent_with_args
BATCH_LOG_EVERY = 10_000   # progress log interval


def load_and_join(data_dir: str, limit: int | None) -> pd.DataFrame:
    """Read the relevant Olist CSVs and join into one flat dataframe."""
    orders = pd.read_csv(f"{data_dir}/olist_orders_dataset.csv",
                          parse_dates=["order_purchase_timestamp", "order_approved_at",
                                       "order_delivered_customer_date",
                                       "order_estimated_delivery_date"])
    customers = pd.read_csv(f"{data_dir}/olist_customers_dataset.csv")
    items = pd.read_csv(f"{data_dir}/olist_order_items_dataset.csv")
    payments = pd.read_csv(f"{data_dir}/olist_order_payments_dataset.csv")
    products = pd.read_csv(f"{data_dir}/olist_products_dataset.csv")
    cat_translation = pd.read_csv(f"{data_dir}/olist_product_category_name_translation.csv")
    sellers = pd.read_csv(f"{data_dir}/olist_sellers_dataset.csv")

    # one row per order: first item's product/seller (keeps the row count
    # aligned with "order" as the unit of work for OLTP/OLAP benchmarking)
    items_first = items.sort_values("order_item_id").groupby("order_id").first().reset_index()
    payments_agg = payments.groupby("order_id")["payment_value"].sum().reset_index()

    df = (orders
          .merge(customers, on="customer_id", how="left")
          .merge(items_first[["order_id", "product_id", "seller_id", "freight_value"]],
                 on="order_id", how="left")
          .merge(payments_agg, on="order_id", how="left")
          .merge(products[["product_id", "product_category_name"]], on="product_id", how="left")
          .merge(cat_translation, on="product_category_name", how="left")
          .merge(sellers[["seller_id", "seller_state"]], on="seller_id", how="left"))

    df = df.rename(columns={"product_category_name_english": "product_category"})

    keep = ["order_id", "customer_id", "customer_state", "customer_city",
            "order_status", "order_purchase_timestamp", "order_approved_at",
            "order_delivered_customer_date", "order_estimated_delivery_date",
            "payment_value", "freight_value", "product_category", "seller_state"]
    df = df[keep].dropna(subset=["customer_state", "order_purchase_timestamp"])

    if limit:
        df = df.head(limit)
    return df


def to_py(val):
    """Convert NaN/NaT to None; leave everything else as-is for the driver."""
    if pd.isna(val):
        return None
    return val


def ingest(df: pd.DataFrame, session) -> None:
    insert_stmt = session.prepare("""
        INSERT INTO orders_by_state (
            customer_state, order_purchase_timestamp, order_id, customer_id,
            customer_city, order_status, order_approved_at,
            order_delivered_customer_date, order_estimated_delivery_date,
            payment_value, freight_value, product_category, seller_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    freight_stmt = session.prepare("""
        INSERT INTO freight_by_month (customer_state, year_month, order_id, freight_value)
        VALUES (?, ?, ?, ?)
    """)

    delivery_stmt = session.prepare("""
        INSERT INTO seller_region_delivery (seller_state, order_purchase_timestamp,
            order_id, delivery_days)
        VALUES (?, ?, ?, ?)
    """)

    counter_stmt = session.prepare("""
        UPDATE customer_order_counts SET order_count = order_count + 1
        WHERE customer_state = ? AND customer_id = ?
    """)

    rows = []
    freight_rows = []
    delivery_rows = []
    counter_rows = []

    for _, r in df.iterrows():
        payment_value = Decimal(str(r["payment_value"])) if pd.notna(r["payment_value"]) else Decimal("0")
        freight_value = Decimal(str(r["freight_value"])) if pd.notna(r["freight_value"]) else Decimal("0")

        rows.append((
            r["customer_state"], r["order_purchase_timestamp"].to_pydatetime(),
            r["order_id"], r["customer_id"], to_py(r["customer_city"]),
            to_py(r["order_status"]),
            r["order_approved_at"].to_pydatetime() if pd.notna(r["order_approved_at"]) else None,
            r["order_delivered_customer_date"].to_pydatetime() if pd.notna(r["order_delivered_customer_date"]) else None,
            r["order_estimated_delivery_date"].to_pydatetime() if pd.notna(r["order_estimated_delivery_date"]) else None,
            payment_value, freight_value, to_py(r["product_category"]), to_py(r["seller_state"]),
        ))

        year_month = r["order_purchase_timestamp"].strftime("%Y-%m")
        freight_rows.append((r["customer_state"], year_month, r["order_id"], freight_value))

        if pd.notna(r["order_delivered_customer_date"]) and pd.notna(r["seller_state"]):
            delivery_days = (r["order_delivered_customer_date"] - r["order_purchase_timestamp"]).days
            delivery_rows.append((r["seller_state"], r["order_purchase_timestamp"].to_pydatetime(),
                                   r["order_id"], delivery_days))

        counter_rows.append((r["customer_state"], r["customer_id"]))

    print(f"Loading {len(rows):,} rows into orders_by_state ...")
    _run_concurrent(session, insert_stmt, rows)

    print(f"Loading {len(freight_rows):,} rows into freight_by_month ...")
    _run_concurrent(session, freight_stmt, freight_rows)

    print(f"Loading {len(delivery_rows):,} rows into seller_region_delivery ...")
    _run_concurrent(session, delivery_stmt, delivery_rows)

    print(f"Updating counters for {len(counter_rows):,} rows in customer_order_counts ...")
    _run_concurrent(session, counter_stmt, counter_rows)


def _run_concurrent(session, stmt, params_list):
    if not params_list:
        return
    start = time.time()
    results = execute_concurrent_with_args(
        session, stmt, params_list, concurrency=CONCURRENCY, raise_on_first_error=False
    )
    errors = [r for success, r in results if not success]
    elapsed = time.time() - start
    print(f"  done in {elapsed:.1f}s | {len(params_list) - len(errors):,} ok | {len(errors)} errors")
    for _, err in list(zip(errors, errors))[:5]:
        print(f"  sample error: {err}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True, help="Directory containing Olist CSV files")
    parser.add_argument("--limit", type=int, default=None,
                         help="Optional row cap, e.g. 10000 / 50000 for scaling tests")
    parser.add_argument("--hosts", nargs="+", default=CASSANDRA_HOSTS)
    args = parser.parse_args()

    print(f"Reading + joining Olist CSVs from {args.data_dir} ...")
    df = load_and_join(args.data_dir, args.limit)
    print(f"Prepared {len(df):,} order rows for ingestion.")

    cluster = Cluster(args.hosts)
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS olist_benchmark
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace(KEYSPACE)

    ingest(df, session)

    cluster.shutdown()
    print("Ingestion complete.")


if __name__ == "__main__":
    sys.exit(main())
