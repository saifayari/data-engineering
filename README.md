# Reliable Big Data pipelines: Airflow + dbt demo

A small daily-sales pipeline that shows four reliability ideas:
recovery after failure, automated data quality, lineage/observability,
and the limits of retries.

```
Faker CSV -> load_raw -> dbt freshness -> dbt staging -> dbt TEST (gate) -> dbt marts
                                                           |
                                                           +-> quality_report
```

## Quick start

```bash
# Linux only: avoid permission issues on mounted folders
echo "AIRFLOW_UID=$(id -u)" > .env

make up          # builds the image (a few minutes the first time), starts everything
# Airflow UI: http://localhost:8080  (admin / admin)
```

## Demo script (about 5 minutes)

| Step | Command | What to show |
|---|---|---|
| 1. Happy path | `make clean-run` | All tasks green in the Airflow grid |
| 2. Transient failure | `make transient-run` | `load_raw` fails on try 1, retries, succeeds. Open the task log. |
| 3. Bad data | `make bad-run` | `load_raw` succeeds (silent problem), `dbt_test` fails 3 times identically, `dbt_marts` is skipped. `quality_report` lists the failed tests. |
| 4. Idempotency | `make rerun` | Same date reloaded: "replaced 2000 existing rows", no duplicates |
| 5. Lineage | `make docs` (http://localhost:8081) or `make lineage` / `make impact` | Upstream and downstream of a model |
| 6. Check the gate | `make psql` then `select order_date, sum(orders) from marts.fct_daily_sales group by 1;` | The bad day is in `raw.orders` but NOT in the marts |

Each run takes about a minute (retry delays are 10s with backoff).
Each `make` target uses a unique run id, so you can repeat any scenario.

## How the four questions map to the code

| Question | Where to look |
|---|---|
| Recovery after failure | `dags/daily_sales.py` (`retries`, backoff, callbacks); `scripts/load_orders.py` (DELETE + COPY in one transaction = idempotent) |
| Automatic data quality | `dbt/models/staging/schema.yml` (tests), `sources.yml` (freshness), `macros/non_negative.sql` (custom test) |
| Lineage and observability | `dbt docs`, `dbt ls --select +model`, `scripts/summarize_dbt_results.py`, `on_retry_callback` / `on_failure_callback` |
| Limits of retries | Scenario 3: `dbt_test` is retried 3 times and fails the same way each time |

## Injected defects and the test that catches each

| Defect | Test | Severity |
|---|---|---|
| Negative `amount` | `non_negative` | error (blocks) |
| Null `order_id` | `not_null` | error (blocks) |
| Duplicate rows | `unique` | error (blocks) |
| Orphan `customer_id` | `relationships` | error (blocks) |
| Unknown `status` | `accepted_values` | warn (alerts only) |

## Slack alerts (optional)

Set `SLACK_WEBHOOK_URL` in the `environment:` block of `docker-compose.yml`.
Without it, alerts are written to the task log as `ALERT: ...`.

## Layout

```
dags/daily_sales.py            the pipeline
scripts/generate_orders.py     fake source system (Faker)
scripts/load_orders.py         idempotent loader (+ simulated transient failure)
scripts/summarize_dbt_results.py   data quality report from dbt's run_results.json
dbt/                           dbt project (sources, staging, marts, tests, macros)
warehouse/init.sql             creates the schemas
```
