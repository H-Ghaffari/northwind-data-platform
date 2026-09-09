"""Record what each task did in ETL_Settings.ETL_Log.

Airflow already records whether a task succeeded. What it does not record
is what the data did: how many rows moved, into which table, and how long
that took. When someone asks why last night's figures look low, "the task
was green" is not an answer.

These functions are wired in as Airflow callbacks, so every task writes a
row without the job code having to know about logging at all.

Deliberately standalone: it reads the environment directly and imports
nothing from the Spark jobs. Airflow reparses every DAG file on a short
interval, and pulling pyspark into that path would make each parse cost
seconds instead of milliseconds.

Failures here are swallowed. A pipeline that dies because its logging table
is unreachable has made observability worse, not better.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

import pymssql

log = logging.getLogger("northwind.etl_log")


def _connect():
    return pymssql.connect(
        server=os.environ.get("OP_HOST", "northwind_op"),
        port=os.environ.get("OP_PORT", "1433"),
        user=os.environ.get("OP_USER", "sa"),
        password=os.environ.get("OP_PASSWORD", ""),
        database="ETL_Settings",
        timeout=10,
        login_timeout=10,
    )


def write_log(
    dag_id: str,
    task_id: str,
    target_object: str | None = None,
    rows_read: int | None = None,
    rows_written: int | None = None,
    status: str = "SUCCESS",
    message: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> None:
    """Insert one row describing a task execution."""
    try:
        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO dbo.ETL_Log "
                    "(dag_id, task_id, target_object, rows_read, rows_written, "
                    " status, message, started_at, finished_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        dag_id, task_id, target_object,
                        rows_read, rows_written, status,
                        # The column is NVARCHAR(MAX), but a full stack trace
                        # still does not belong in a summary table.
                        (message or "")[:3900],
                        started_at, finished_at or datetime.utcnow(),
                    ),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        log.warning("Could not write to ETL_Log: %s", exc)


# ---------------------------------------------------------------------------
# Airflow callbacks
# ---------------------------------------------------------------------------

def _timings(task_instance) -> tuple[datetime | None, datetime | None]:
    return (
        getattr(task_instance, "start_date", None),
        getattr(task_instance, "end_date", None) or datetime.utcnow(),
    )


def on_success(context: dict[str, Any]) -> None:
    """Record a successful task, taking its return value as a row count.

    Every loader in this project returns the number of rows it wrote, so the
    XCom return value is the row count with no extra plumbing.
    """
    task_instance = context["task_instance"]
    started_at, finished_at = _timings(task_instance)

    rows_written = None
    try:
        returned = task_instance.xcom_pull(task_ids=task_instance.task_id)
        if isinstance(returned, int):
            rows_written = returned
    except Exception:
        pass

    write_log(
        dag_id=context["dag"].dag_id,
        task_id=task_instance.task_id,
        target_object=task_instance.task_id,
        rows_written=rows_written,
        status="SUCCESS",
        started_at=started_at,
        finished_at=finished_at,
    )


def on_failure(context: dict[str, Any]) -> None:
    """Record a failed task and why it failed."""
    task_instance = context["task_instance"]
    started_at, finished_at = _timings(task_instance)

    write_log(
        dag_id=context["dag"].dag_id,
        task_id=task_instance.task_id,
        target_object=task_instance.task_id,
        status="FAILED",
        message=str(context.get("exception", "")),
        started_at=started_at,
        finished_at=finished_at,
    )
