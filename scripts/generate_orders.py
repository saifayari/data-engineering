"""
Fake source system: generates one CSV of orders per day (Faker + pandas).

  python generate_orders.py --date 2026-10-10            # clean day
  python generate_orders.py --date 2026-10-12 --bad      # corrupted day

The same date always produces the same file (idempotent demo).
Output: <out-dir>/orders_<date>.csv  (+ customers.csv, created once)
"""
import argparse
import os
import random
from pathlib import Path

import pandas as pd
from faker import Faker

SEED = 42
N_CUSTOMERS = 500
N_ORDERS = 2000
STATUSES = ["paid", "shipped", "cancelled", "refunded"]


def make_customers(fake: Faker) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "customer_id": range(1, N_CUSTOMERS + 1),
            "name": [fake.name() for _ in range(N_CUSTOMERS)],
            "email": [fake.email() for _ in range(N_CUSTOMERS)],
            "country": [fake.country_code() for _ in range(N_CUSTOMERS)],
        }
    )


def make_orders(date: str) -> pd.DataFrame:
    day_seed = SEED + int(date.replace("-", ""))
    random.seed(day_seed)
    base_id = int(date.replace("-", "")) * 10_000  # unique order_id per day
    return pd.DataFrame(
        {
            "order_id": [base_id + i for i in range(N_ORDERS)],
            "customer_id": [random.randint(1, N_CUSTOMERS) for _ in range(N_ORDERS)],
            "order_date": date,
            "status": random.choices(STATUSES, weights=[60, 25, 10, 5], k=N_ORDERS),
            "amount": [round(random.uniform(5, 500), 2) for _ in range(N_ORDERS)],
        }
    )


def corrupt(df: pd.DataFrame) -> pd.DataFrame:
    """Inject the defects that the dbt tests are designed to catch."""
    df = df.copy()
    n = len(df)
    df["order_id"] = df["order_id"].astype("Int64")  # keep ints when nulls are added

    df.loc[df.sample(frac=0.02, random_state=1).index, "amount"] *= -1          # non_negative
    df.loc[df.sample(frac=0.01, random_state=2).index, "order_id"] = pd.NA      # not_null
    df.loc[df.sample(frac=0.02, random_state=3).index, "status"] = "pending_unknown"  # accepted_values
    df.loc[df.sample(frac=0.01, random_state=4).index, "customer_id"] = 99999   # relationships
    df = pd.concat([df, df.sample(frac=0.01, random_state=5)])                  # unique

    print(f"Corrupted file: {n} -> {len(df)} rows "
          "(negative amounts, null ids, bad status, orphan customers, duplicates)")
    return df


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--bad", action="store_true", help="inject data quality problems")
    parser.add_argument("--out-dir", default=os.environ.get("DATA_DIR", "data"))
    args = parser.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    customers_path = out / "customers.csv"
    if not customers_path.exists():
        Faker.seed(SEED)
        make_customers(Faker()).to_csv(customers_path, index=False)
        print(f"Wrote {customers_path}")

    df = make_orders(args.date)
    if args.bad:
        df = corrupt(df)

    path = out / f"orders_{args.date}.csv"
    df.to_csv(path, index=False)
    print(f"Wrote {path} ({len(df)} rows, {'BAD' if args.bad else 'clean'})")


if __name__ == "__main__":
    main()
