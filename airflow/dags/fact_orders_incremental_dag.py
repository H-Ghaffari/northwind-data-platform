"""Apply CDC changes to FactOrders every 30 minutes.

The cadence comes from the brief: "30 Minutes => Captured Instance". The
window between runs is what makes inferred members necessary — a fact can
arrive up to a day before the dimension load that describes it.

max_active_runs is 1 because two concurrent runs would read overlapping LSN
windows and race on the watermark. catchup is False because a missed run
does not need replaying: the next run's window simply starts from the same
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

sys.path.insert(0, "/opt/spark-jobs/jobs")

from dw.fact_orders import run_one  # noqa: E402

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=25),
}


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
        python_callable=run_one,
        op_args=["incremental"],
    )

    end = EmptyOperator(task_id="end")

    start >> load >> end
