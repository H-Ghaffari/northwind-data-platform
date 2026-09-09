"""Populate the data lake and register what it holds.

No schedule: the files are deterministic and only need regenerating when
the employee list changes. Triggered by hand, or after a dimension load
that adds someone new.

Depends on DimEmployees being populated — the catalogue keys on
employee_alternate_key, and generating avatars for an empty dimension would
produce an empty lake without saying why.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/spark-jobs/jobs")
sys.path.insert(0, "/opt/airflow/dags")

from dag_common.datasets import LAKE_PHOTOS  # noqa: E402
from lake.photo_generator import generate_photos, register_catalogue, verify  # noqa: E402

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}


def _generate(**context):
    """Write the files and hand the catalogue to the next task."""
    catalogue = generate_photos()
    context["ti"].xcom_push(key="catalogue", value=catalogue)
    return len(catalogue)


def _register(**context):
    """Register what the previous task wrote.

    The catalogue travels through XCom rather than being regenerated here:
    regenerating would risk describing different files from the ones on
    disk if anything changed between the two tasks.
    """
    catalogue = context["ti"].xcom_pull(task_ids="generate_photos", key="catalogue")
    return register_catalogue(catalogue or [])


with DAG(
    dag_id="lake_employee_photos",
    description="Generate employee avatars into the data lake and catalogue them",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["northwind", "lake", "unstructured", "manual"],
) as dag:

    start = EmptyOperator(task_id="start")

    generate = PythonOperator(
        task_id="generate_photos",
        python_callable=_generate,
    )

    register = PythonOperator(
        task_id="register_catalogue",
        python_callable=_register,
    )

    check = PythonOperator(
        task_id="verify_catalogue",
        python_callable=verify,
        outlets=[LAKE_PHOTOS],
    )

    end = EmptyOperator(task_id="end")

    start >> generate >> register >> check >> end
