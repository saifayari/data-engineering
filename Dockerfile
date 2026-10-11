FROM apache/airflow:2.10.5-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# dbt lives in its own virtualenv to avoid dependency conflicts with Airflow
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.8.9 dbt-postgres==1.8.2
