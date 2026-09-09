"""Nightly reload of every dimension source into the staging layer.

Runs at 22:00 as the brief specifies. One task per staging table rather
than a single task doing all eight: a failure in the products load should
not force customers to be reloaded too, and the Airflow UI shows exactly
which source is broken.

The tasks are independent — all eight read from OP and write to staging
with nothing shared between them — so they run in parallel.

On completion this DAG marks STAGING_DIMENSIONS as updated, which triggers
the warehouse load. The two are no longer chained by clock time: if this
DAG runs long, the next one waits rather than building the warehouse from
half-filled tables.

The job module is imported inside the callable, not at the top of the file.
Airflow reparses every DAG file on a short interval, and op_to_staging
pulls in pyspark — importing it at parse time would cost seconds on every
scheduler loop for code only needed when a task actually runs.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/dags")

from dag_common.datasets import STAGING_DIMENSIONS  # noqa: E402
from dag_common.etl_logger import on_failure, on_success  # noqa: E402

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    # Every task records what it moved in ETL_Settings.ETL_Log. Airflow's own
    # history says whether a task ran; this says what the data did.
    "on_success_callback": on_success,
    "on_failure_callback": on_failure,
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


def load_source(source: str) -> int:
    """Run one staging loader. Imports the job module at execution time."""
    if "/opt/spark-jobs/jobs" not in sys.path:
        sys.path.insert(0, "/opt/spark-jobs/jobs")
    from staging.op_to_staging import run_one
    return run_one(source)


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

    load_tasks = [
        PythonOperator(
            task_id=f"load_{source}",
            python_callable=load_source,
            op_args=[source],
        )
        for source in SOURCES
    ]

    # The dataset is marked on a separate terminal task, not on the loaders:
    # the warehouse should wait for all eight sources, not start after
    # whichever finishes first.
    staging_ready = EmptyOperator(
        task_id="staging_ready",
        outlets=[STAGING_DIMENSIONS],
    )

    start >> load_tasks >> staging_ready
