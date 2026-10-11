.PHONY: up down reset clean-run transient-run bad-run rerun docs lineage impact psql logs

AF = docker compose exec airflow-scheduler
RUN_ID = demo_$$(date +%H%M%S)

up:
	docker compose up airflow-init && docker compose up -d
	@echo "Airflow UI: http://localhost:8080  (admin / admin)"

down:
	docker compose down

reset:
	docker compose down -v

# Scenario 1: everything green
clean-run:
	$(AF) airflow dags trigger daily_sales -e 2026-10-10 -r $(RUN_ID)

# Scenario 2: transient failure -> Airflow retries -> success
transient-run:
	$(AF) airflow dags trigger daily_sales -e 2026-10-11 -r $(RUN_ID) --conf '{"fail_once": true}'

# Scenario 3: bad data -> retries cannot help -> quality gate blocks marts
bad-run:
	$(AF) airflow dags trigger daily_sales -e 2026-10-12 -r $(RUN_ID) --conf '{"inject_bad_data": true}'

# Idempotency: re-run a day that was already loaded (no duplicates)
rerun:
	$(AF) airflow dags trigger daily_sales -e 2026-10-10 -r $(RUN_ID)

# Lineage
docs:
	docker compose --profile docs up -d dbt-docs
	@echo "dbt docs: http://localhost:8081  (click the lineage graph icon)"

lineage:
	$(AF) bash -c 'cd /opt/airflow/dbt && $$DBT_BIN ls --select +fct_daily_sales'

impact:
	$(AF) bash -c 'cd /opt/airflow/dbt && $$DBT_BIN ls --select stg_orders+'

psql:
	docker compose exec warehouse psql -U warehouse -d warehouse
