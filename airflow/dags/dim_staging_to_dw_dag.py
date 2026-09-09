"""Load the dimensions from staging into the warehouse.

Unlike the OP → Staging DAG, the order here is not cosmetic. Every edge
below exists because the downstream task looks up a surrogate key the
upstream task creates:

    geography   →  suppliers, customer, employees   (address lookup)
    suppliers   →  products                         (supplier_key lookup)
    employees   →  employee_hierarchy               (self-reference, pass two)
    employees   ┐
    territories ┘→ fact_employee_territories        (both keys)

Getting this wrong does not fail loudly: the lookup simply returns nothing
and the rows land with key 0, which looks like a data problem rather than
an ordering one.

Triggered by the staging DAG through a dataset rather than by a clock. The
previous version ran at 22:30 on the assumption that the 22:00 staging load
would be finished — an assumption that holds until it does not.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/dags")

from dag_common.datasets import DW_DIMENSIONS, STAGING_DIMENSIONS  # noqa: E402
from dag_common.etl_logger import on_failure, on_success  # noqa: E402

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
    "on_success_callback": on_success,
    "on_failure_callback": on_failure,
}


def load_dimension(name: str) -> int:
    """Run one warehouse loader. Imports the job module at execution time."""
    if "/opt/spark-jobs/jobs" not in sys.path:
        sys.path.insert(0, "/opt/spark-jobs/jobs")
    from dw.staging_to_dw import run_one
    return run_one(name)


def _task(task_id: str) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=load_dimension,
        op_args=[task_id],
    )


with DAG(
    dag_id="dim_staging_to_dw",
    description="Load dimensions from PostgreSQL staging into the ClickHouse star schema",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=[STAGING_DIMENSIONS],
    catchup=False,
    max_active_runs=1,
    tags=["northwind", "dw", "dimensions", "scd"],
) as dag:

    start = EmptyOperator(task_id="start")

    geography = _task("dim_geography")
    shippers = _task("dim_shippers")
    territories = _task("dim_territories")
    suppliers = _task("dim_suppliers")
    customer = _task("dim_customer")
    products = _task("dim_products")
    employees = _task("dim_employees")
    hierarchy = _task("employee_hierarchy")
    bridge = _task("fact_employee_territories")

    # Every dimension is in place; the fact loads can resolve keys now.
    dimensions_ready = EmptyOperator(
        task_id="dimensions_ready",
        outlets=[DW_DIMENSIONS],
    )

    # Independent of everything else.
    start >> [geography, shippers, territories]

    # Address lookups need geography in place first.
    geography >> [suppliers, customer, employees]

    # supplier_key must exist before products can resolve it.
    suppliers >> products

    # parent_employee_key is a surrogate key, so every employee row must
    # exist before the hierarchy can be resolved.
    employees >> hierarchy

    # The bridge needs both dimensions it points at.
    [hierarchy, territories] >> bridge

    [products, customer, shippers, bridge] >> dimensions_ready
