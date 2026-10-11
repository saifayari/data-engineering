"""
daily_sales: generate -> load -> freshness -> staging -> TESTS (gate) -> marts

Trigger parameters (Airflow UI: Trigger DAG w/ config):
  inject_bad_data : source file contains data quality problems
  fail_once       : load fails once with a transient error (retry succeeds)
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

SCRIPTS = "/opt/airflow/scripts"
DATA = "/opt/airflow/data"
DBT = "cd /opt/airflow/dbt && $DBT_BIN"


def _post_slack(text: str) -> None:
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        return
    req = urllib.request.Request(
        url, data=json.dumps({"text": text}).encode(),
        headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=5)


def notify_retry(context):
    ti = context["task_instance"]
    log.warning("RETRY: %s.%s failed on try %s, will retry. Exception: %s",
                ti.dag_id, ti.task_id, ti.try_number, context.get("exception"))


def notify_failure(context):
    ti = context["task_instance"]
    msg = (f":rotating_light: {ti.dag_id}.{ti.task_id} FAILED for {context['ds']} "
           f"after {ti.try_number} tries. {ti.log_url}")
    log.error("ALERT: %s", msg)
    try:
        _post_slack(msg)
    except Exception:  # alerting must never break the pipeline
        log.exception("Could not send Slack alert")


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=1),
    "on_retry_callback": notify_retry,
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="daily_sales",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,          # use `airflow dags backfill` to replay missed days
    max_active_runs=1,
    default_args=default_args,
    params={
        "inject_bad_data": Param(False, type="boolean"),
        "fail_once": Param(False, type="boolean"),
    },
    tags=["demo", "reliability"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=(f"python {SCRIPTS}/generate_orders.py --date {{{{ ds }}}} "
                      f"--out-dir {DATA} "
                      "{% if params.inject_bad_data %}--bad{% endif %}"),
    )

    load_raw = BashOperator(
        task_id="load_raw",
        bash_command=(f"python {SCRIPTS}/load_orders.py --date {{{{ ds }}}} "
                      f"--data-dir {DATA} "
                      "{% if params.fail_once %}--fail-once{% endif %}"),
    )

    dbt_freshness = BashOperator(
        task_id="dbt_freshness",
        bash_command=f"{DBT} source freshness",
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"{DBT} run --select staging",
    )

    # Quality gate: if any error-severity test fails, marts are NOT built.
    # run_results.json is copied so the report survives the later `dbt run`.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(f"{DBT} test --select staging || rc=$?; "
                      "cp -f target/run_results.json target/test_results.json; "
                      "exit ${rc:-0}"),
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=f"{DBT} run --select marts",
    )

    quality_report = BashOperator(
        task_id="quality_report",
        bash_command=f"python {SCRIPTS}/summarize_dbt_results.py",
        trigger_rule="all_done",  # report even when the gate failed
        retries=0,
    )

    generate_data >> load_raw >> dbt_freshness >> dbt_staging >> dbt_test
    dbt_test >> [dbt_marts, quality_report]
