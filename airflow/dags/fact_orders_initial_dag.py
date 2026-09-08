"""One-off seed of FactOrders.

No schedule: this truncates the fact table and reloads every order from the
source, which would discard anything the incremental DAG has applied. It is
meant to be triggered by hand — when the warehouse is first built, or after
a change that invalidates what is already loaded.

Kept as a DAG rather than a script so the seed appears in the same run
history as everything else, with the same logs and the same retry
behaviour.
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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


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

    load = PythonOperator(
        task_id="initial_load",
        python_callable=run_one,
        op_args=["initial"],
    )

    end = EmptyOperator(task_id="end")

    start >> load >> end
