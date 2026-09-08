"""Connection settings for every layer of the pipeline.

Values come from the environment so the same code runs unchanged in the
Airflow container, in a local shell, and in a test. Nothing here reads a
config file: the container already has the environment populated by
docker-compose, and duplicating that into a file only creates a second
source of truth to keep in sync.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(
            f"Required environment variable {name} is not set. "
            f"Check the environment block for this service in docker-compose.yml."
        )
    return value


@dataclass(frozen=True)
class OpConfig:
    """SQL Server — the operational source."""

    host: str = os.environ.get("OP_HOST", "northwind_op")
    port: int = int(os.environ.get("OP_PORT", "1433"))
    database: str = os.environ.get("OP_DB", "Northwind")
    user: str = os.environ.get("OP_USER", "sa")
    password: str = os.environ.get("OP_PASSWORD", "")

    @property
    def jdbc_url(self) -> str:
        # encrypt=true is the default from driver 10 onwards; the container
        # uses a self-signed certificate, so trust must be explicit.
        return (
            f"jdbc:sqlserver://{self.host}:{self.port};"
            f"databaseName={self.database};"
            f"encrypt=true;trustServerCertificate=true"
        )

    @property
    def jdbc_properties(self) -> dict[str, str]:
        return {
            "user": self.user,
            "password": self.password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }


@dataclass(frozen=True)
class StagingConfig:
    """PostgreSQL — the integration area."""

    host: str = os.environ.get("STAGING_HOST", "northwind_staging")
    port: int = int(os.environ.get("STAGING_PORT", "5432"))
    database: str = os.environ.get("STAGING_DB", "Staging_Northwind")
    user: str = os.environ.get("STAGING_USER", "staging_admin")
    password: str = os.environ.get("STAGING_PASSWORD", "")

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def jdbc_properties(self) -> dict[str, str]:
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }


@dataclass(frozen=True)
class DwConfig:
    """ClickHouse — the star schema.

    Loaded over HTTP with clickhouse-connect rather than JDBC. The JDBC
    driver works, but the Python client handles batching and type coercion
    for the volumes involved here with far less ceremony.
    """

    host: str = os.environ.get("DW_HOST", "northwind_dw")
    port: int = int(os.environ.get("DW_HTTP_PORT", "8123"))
    database: str = os.environ.get("DW_DB", "NorthwindDW")
    user: str = os.environ.get("DW_USER", "dw_admin")
    password: str = os.environ.get("DW_PASSWORD", "")


OP = OpConfig()
STAGING = StagingConfig()
DW = DwConfig()

# Directory holding the JDBC jars, baked into the Airflow image.
SPARK_JARS_DIR = os.environ.get("SPARK_JARS_DIR", "/opt/spark-jars")
