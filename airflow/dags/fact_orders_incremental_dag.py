"""Apply CDC changes to FactOrders, in two stages.

The cadence comes from the brief: "30 Minutes => Captured Instance".

Two tasks rather than one, mirroring the reference project's split between
packages 11/12 (OP → Staging) and package 13 (Staging → DW):

    op_to_staging   reads CDC, snapshots the affected orders, lands six tables
    staging_to_dw   resolves keys, applies to the warehouse, moves watermark

The split earns its keep when the second half fails. Everything CDC
reported is already in staging, so the retry resolves keys again without
going back to the source — and the watermark has not moved, so nothing is
lost either way.

The LSN window travels between the two through XCom. Recomputing it in the
second task would be wrong: changes may arrive in the seconds between them,
and advancing the watermark past unprocessed changes would skip them.

Scheduled on a clock rather than on the dimension dataset, deliberately.
Facts must keep flowing every half hour whether or not the dimensions have
reloaded; that gap is exactly what inferred members exist to absorb.

max_active_runs is 1 because two concurrent runs would truncate each
other's staging tables mid-flight. catchup is False because a missed run
needs no replay: the next window starts from the same watermark.
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


def _op_to_staging(**context) -> int:
    """Stage one: land the CDC window and snapshot the orders it touched."""
    if "/opt/spark-jobs/jobs" not in sys.path:
        sys.path.insert(0, "/opt/spark-jobs/jobs")
    from staging.op_to_staging_facts import run_incremental

    window = run_incremental()
    context["ti"].xcom_push(key="cdc_window", value=window)
    return window["total_changes"]


def _staging_to_dw(**context) -> int:
    """Stage two: resolve keys, apply to the warehouse, move the watermark."""
    if "/opt/spark-jobs/jobs" not in sys.path:
        sys.path.insert(0, "/opt/spark-jobs/jobs")
    from dw.staging_to_dw_facts import run_incremental

    window = context["ti"].xcom_pull(
        task_ids="op_to_staging_facts", key="cdc_window"
    )
    return run_incremental(window)


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

    op_to_staging = PythonOperator(
        task_id="op_to_staging_facts",
        python_callable=_op_to_staging,
    )

    staging_to_dw = PythonOperator(
        task_id="staging_to_dw_facts",
        python_callable=_staging_to_dw,
        outlets=[DW_FACTS],
    )

    end = EmptyOperator(task_id="end")

    start >> op_to_staging >> staging_to_dw >> end
