"""Dataset definitions shared by every DAG.

A Dataset is a name two DAGs agree on: one declares it produces the name,
the other schedules itself on it. Airflow then triggers the consumer when
the producer finishes, instead of the consumer running on a clock and
hoping the producer got there first.

Time-based chaining is what this replaces. Running the warehouse load at
22:30 because the staging load starts at 22:00 works right up until the
staging load takes 35 minutes, at which point the warehouse quietly builds
itself from half-loaded tables.

The scheme is `northwind`, not `postgres` or `clickhouse`. Airflow
validates the URI format of schemes it recognises and expects a full
database, schema and table path for them — a constraint that makes no sense
here, since these names stand for "every dimension has landed", not for one
table. A project-specific scheme sidesteps that validation, which Airflow 3
turns from a warning into an error.

Defined once, here: Airflow matches these by string equality, so a typo in
one DAG would silently break the link rather than fail.
"""

from __future__ import annotations

from airflow.datasets import Dataset

# --- Staging -------------------------------------------------------------
# Produced by dim_op_to_staging once every dimension source has landed.
STAGING_DIMENSIONS = Dataset("northwind://staging/dimensions")

# --- Warehouse -----------------------------------------------------------
# Produced by dim_staging_to_dw once all dimensions carry surrogate keys.
DW_DIMENSIONS = Dataset("northwind://dw/dimensions")

# Produced by the fact loads.
DW_FACTS = Dataset("northwind://dw/facts")

# --- Lake ----------------------------------------------------------------
# Produced by lake_employee_photos.
LAKE_PHOTOS = Dataset("northwind://lake/employee-photos")