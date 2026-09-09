"""Build the platform from empty containers.

Everything here is DDL and one-off configuration: create the databases,
load the Northwind sample, create the staging and warehouse schemas, enable
CDC. Run once when the stack is first brought up.

No schedule, by design. Structure is created once; recreating it nightly
would be pointless at best and destructive if anyone ever added a DROP.
This is infrastructure, and the only reason it lives in Airflow at all is
so that a person setting the project up has one place to press rather than
four scripts to run in the right order.

Every step is idempotent, so a partial failure can be fixed and the DAG
rerun without unpicking anything first.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

# The scheduler container reaches the other services over the compose
# network, so these run sqlcmd and clickhouse-client against hostnames
# rather than through the host port mappings.
SQLCMD = (
    "/opt/mssql-tools18/bin/sqlcmd -S $OP_HOST,$OP_PORT "
    "-U $OP_USER -P $OP_PASSWORD -C -b"
)


with DAG(
    dag_id="00_bootstrap_platform",
    description="One-off: create databases, schemas and CDC. Trigger manually.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["northwind", "setup", "manual"],
) as dag:

    start = EmptyOperator(task_id="start")

    # --- OP layer --------------------------------------------------------
    create_op_databases = BashOperator(
        task_id="create_op_databases",
        bash_command=f'{SQLCMD} -d master -i /opt/sql/01_op/00_create_database.sql',
    )

    load_northwind = BashOperator(
        task_id="load_northwind_sample",
        bash_command=f'{SQLCMD} -d Northwind -i /opt/sql/01_op/instnwnd.sql',
    )

    create_etl_settings = BashOperator(
        task_id="create_etl_settings",
        bash_command=f'{SQLCMD} -d master -i /opt/sql/04_etl_settings/00_create_etl_settings.sql',
    )

    # --- Staging layer ---------------------------------------------------
    create_staging_schema = BashOperator(
        task_id="create_staging_schema",
        bash_command=(
            'PGPASSWORD="$STAGING_PASSWORD" psql '
            '-h "$STAGING_HOST" -p "$STAGING_PORT" '
            '-U "$STAGING_USER" -d "$STAGING_DB" '
            '-v ON_ERROR_STOP=1 '
            '-f /opt/sql/02_staging/00_create_staging_tables.sql'
        ),
    )

    # --- DW layer --------------------------------------------------------
    # clickhouse-client is not in this image, so the HTTP interface is used
    # instead. It takes the same SQL and needs nothing installed.
    create_dw_schema = BashOperator(
        task_id="create_dw_schema",
        bash_command=(
            'curl -sS --fail-with-body '
            '-u "$DW_USER:$DW_PASSWORD" '
            '"http://$DW_HOST:$DW_HTTP_PORT/" '
            '--data-binary @/opt/sql/03_dw/00_create_dw_tables.sql'
        ),
    )

    create_lake_catalogue = BashOperator(
        task_id="create_lake_catalogue",
        bash_command=(
            'curl -sS --fail-with-body '
            '-u "$DW_USER:$DW_PASSWORD" '
            '"http://$DW_HOST:$DW_HTTP_PORT/?database=$DW_DB" '
            '--data-binary @/opt/sql/03_dw/01_create_lake_tables.sql'
        ),
    )

    # --- CDC -------------------------------------------------------------
    # Last: it needs the tables it captures to exist.
    enable_cdc = BashOperator(
        task_id="enable_cdc",
        bash_command=f'{SQLCMD} -d Northwind -i /opt/sql/01_op/01_enable_cdc.sql',
    )

    end = EmptyOperator(task_id="end")

    start >> create_op_databases >> load_northwind >> create_etl_settings
    create_etl_settings >> enable_cdc

    start >> create_staging_schema
    start >> create_dw_schema >> create_lake_catalogue

    [enable_cdc, create_staging_schema, create_lake_catalogue] >> end
