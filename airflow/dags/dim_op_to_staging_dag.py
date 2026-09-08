"""Nightly reload of every dimension source into the staging layer.

Runs at 22:00 as the brief specifies. One task per staging table rather
than a single task doing all eight: a failure in the products load should
not force customers to be reloaded too, and the Airflow UI shows exactly
which source is broken.

The tasks are independent — all eight read from OP and write to staging
with nothing shared between them — so they run in parallel. The dependency
edges drawn below are ordering for readability, not necessity; the real
dependency graph appears in the Staging → DW DAG, where surrogate keys
must exist before anything can look them up.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# The jobs directory is mounted into the container but is not on the default
# import path, so it is added here rather than relying on PYTHONPATH being
# set for every execution context.
sys.path.insert(0, "/opt/spark-jobs/jobs")

from staging.op_to_staging import run_one  # noqa: E402

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

SOURCES = [
    "geography",
    "suppliers",
    "products",
    "customers",
    "employees",
    "shippers",
    "territories",
    "employee_territories",
]


with DAG(
    dag_id="dim_op_to_staging",
    description="Full reload of dimension sources from SQL Server into PostgreSQL",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 22 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["northwind", "staging", "dimensions"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    load_tasks = [
        PythonOperator(
            task_id=f"load_{source}",
            python_callable=run_one,
            op_args=[source],
        )
        for source in SOURCES
    ]

    start >> load_tasks >> end
