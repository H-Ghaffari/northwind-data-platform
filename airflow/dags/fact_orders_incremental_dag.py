"""Apply CDC changes to FactOrders every 30 minutes.

The cadence comes from the brief: "30 Minutes => Captured Instance". The
window between runs is what makes inferred members necessary — a fact can
arrive up to a day before the dimension load that describes it.

Scheduled on a clock rather than on the dimension dataset, deliberately.
The facts must keep flowing every half hour whether or not the dimensions
have reloaded; that gap is exactly what inferred members exist to absorb.
Waiting on DW_DIMENSIONS here would tie a half-hourly job to a nightly one.

max_active_runs is 1 because two concurrent runs would read overlapping LSN
windows and race on the watermark. catchup is False because a missed run
does not need replaying: the next run's window starts from the same
watermark and covers everything that accumulated.

Retries are deliberately generous. The watermark only advances after a
successful write, so a retry reprocesses the same window rather than
skipping it, and the fact table deduplicates on (order_id, product_key).
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/dags")

from dag_common.datasets import DW_FACTS  # noqa: E402
from dag_common.etl_logger import on_failure, on_success  # noqa: E402

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=25),
    "on_success_callback": on_success,
    "on_failure_callback": on_failure,
}


def run_incremental() -> int:
    """Apply the CDC window. Imports the job module at execution time."""
    if "/opt/spark-jobs/jobs" not in sys.path:
        sys.path.insert(0, "/opt/spark-jobs/jobs")
    from dw.fact_orders import run_one
    return run_one("incremental")


with DAG(
    dag_id="fact_orders_incremental",
    description="Apply CDC changes from Orders and Order Details to FactOrders",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["northwind", "dw", "facts", "cdc"],
) as dag:

    start = EmptyOperator(task_id="start")

    load = PythonOperator(
        task_id="incremental_load",
        python_callable=run_incremental,
        outlets=[DW_FACTS],
    )

    end = EmptyOperator(task_id="end")

    start >> load >> end
