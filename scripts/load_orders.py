"""
Idempotent load of one day of orders into the warehouse (raw schema).

  python load_orders.py --date 2026-10-10 [--fail-once]

Idempotency: DELETE + COPY for that date inside ONE transaction.
Re-running the same date never duplicates rows, and a crash mid-load
leaves the table untouched (the transaction rolls back).

--fail-once simulates a transient failure: the first attempt raises a
connection error, the next attempt (Airflow retry) succeeds.
"""
import argparse
import os
import sys
from pathlib import Path

import psycopg2

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id int, name text, email text, country text,
    _loaded_at timestamptz DEFAULT now()
);
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id bigint, customer_id int, order_date date, status text,
    amount numeric(12,2),
    _loaded_at timestamptz DEFAULT now()
);
"""


def connect():
    return psycopg2.connect(
        host=os.environ["WAREHOUSE_HOST"],
        port=os.environ.get("WAREHOUSE_PORT", "5432"),
        dbname=os.environ["WAREHOUSE_DB"],
        user=os.environ["WAREHOUSE_USER"],
        password=os.environ["WAREHOUSE_PASSWORD"],
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "data"))
    parser.add_argument("--fail-once", action="store_true")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    orders_csv = data_dir / f"orders_{args.date}.csv"
    customers_csv = data_dir / "customers.csv"
    if not orders_csv.exists():
        sys.exit(f"Missing source file: {orders_csv}")

    if args.fail_once:
        marker = data_dir / f".fail_once_{args.date}"
        if not marker.exists():
            marker.touch()
            raise ConnectionError(
                "Simulated transient failure: connection reset by peer (will succeed on retry)"
            )
        marker.unlink()  # second attempt: clean up and continue

    with connect() as conn:  # commits on success, rolls back on exception
        with conn.cursor() as cur:
            cur.execute(DDL)

            cur.execute("TRUNCATE raw.customers")
            with open(customers_csv) as f:
                cur.copy_expert(
                    "COPY raw.customers (customer_id, name, email, country) "
                    "FROM STDIN WITH CSV HEADER", f)

            cur.execute("DELETE FROM raw.orders WHERE order_date = %s", (args.date,))
            deleted = cur.rowcount
            with open(orders_csv) as f:
                cur.copy_expert(
                    "COPY raw.orders (order_id, customer_id, order_date, status, amount) "
                    "FROM STDIN WITH CSV HEADER", f)
            cur.execute("SELECT count(*) FROM raw.orders WHERE order_date = %s", (args.date,))
            loaded = cur.fetchone()[0]

    print(f"Loaded {loaded} rows for {args.date} (replaced {deleted} existing rows)")


if __name__ == "__main__":
    main()
