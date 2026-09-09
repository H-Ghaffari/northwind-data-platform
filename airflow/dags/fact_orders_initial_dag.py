"""One-off seed of FactOrders, in two stages.

No schedule: the second task truncates the fact table and reloads every
order, which would discard anything the incremental DAG has applied. Meant
to be triggered by hand — when the warehouse is first built, or after a
change that invalidates what is already loaded.

Same two-stage shape as the incremental DAG, for the same reason: the
warehouse load reads from staging, never from the source, so a failure
resolving keys is retried without touching SQL Server again.

Kept as a DAG rather than a script so the seed appears in the same run
history as everything else, with the same logs and retry behaviour.
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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "on_success_callback": on_success,
    "on_failure_callback": on_failure,
}


def _op_to_staging() -> int:
    """Stage one: snapshot every order and line into staging."""
    if "/opt/spark-jobs/jobs" not in sys.path:
        sys.path.insert(0, "/opt/spark-jobs/jobs")
    from staging.op_to_staging_facts import run_initial

    result = run_initial()
    return result["snapshot"]["staging_order_details"]


def _staging_to_dw() -> int:
    """Stage two: build every fact row from the staging snapshot."""
    if "/opt/spark-jobs/jobs" not in sys.path:
        sys.path.insert(0, "/opt/spark-jobs/jobs")
    from dw.staging_to_dw_facts import run_initial

    return run_initial()


with DAG(
    dag_id="fact_orders_initial_load",
    description="One-off full reload of FactOrders — trigger manually",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["northwind", "dw", "facts", "manual"],
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
