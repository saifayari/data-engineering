"""Print a data quality summary from dbt's run_results.json (observability)."""
import json
import sys
from collections import Counter

path = sys.argv[1] if len(sys.argv) > 1 else "/opt/airflow/dbt/target/test_results.json"
try:
    results = json.load(open(path))["results"]
except FileNotFoundError:
    sys.exit(f"No results file at {path}")

counts = Counter(r["status"] for r in results)
print("=== Data quality report ===")
print("  " + ", ".join(f"{k}: {v}" for k, v in sorted(counts.items())))
for r in results:
    if r["status"] in ("fail", "warn", "error"):
        n = r.get("failures")
        print(f"  [{r['status'].upper():5}] {r['unique_id'].split('.')[2]}  "
              f"({n} failures)" if n is not None else f"  [{r['status'].upper()}] {r['unique_id']}")
