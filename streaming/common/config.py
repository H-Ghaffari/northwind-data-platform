"""Environment-backed configuration for the streaming services.

Every value comes from the environment rather than a config file, so the
producer and consumer read the same credentials the rest of the platform
already has in .env and nothing is duplicated into a second place that can
fall out of step.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"required environment variable {name} is not set")
    return value


@dataclass(frozen=True)
class OpConfig:
    """The operational source — SQL Server, where CDC lives."""
    host: str = os.environ.get("OP_HOST", "northwind_op")
    port: int = int(os.environ.get("OP_PORT", "1433"))
    database: str = os.environ.get("OP_DB", "Northwind")
    settings_db: str = os.environ.get("ETL_SETTINGS_DB", "ETL_Settings")
    user: str = os.environ.get("OP_USER", "sa")

    @property
    def password(self) -> str:
        return _env("OP_PASSWORD")


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap: str = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    topic_prefix: str = os.environ.get("KAFKA_TOPIC_PREFIX", "nw.cdc")

    # One partition per topic. Message ordering is only guaranteed within a
    # partition, and every consumer here has to apply a dimension's changes
    # in commit order. A single partition makes that structural rather than
    # something the key hashing has to keep getting right.
    partitions: int = int(os.environ.get("KAFKA_TOPIC_PARTITIONS", "1"))
    replication: int = 1  # single broker; nothing to replicate to


@dataclass(frozen=True)
class ProducerConfig:
    poll_seconds: float = float(os.environ.get("STREAM_POLL_SECONDS", "5"))

    # Cap on rows read from one capture instance in one pass. A long outage
    # leaves a large backlog, and reading all of it in a single transaction
    # would hold a connection open for minutes and delay every other source.
    # The loop simply comes back for the rest.
    max_rows_per_pass: int = int(os.environ.get("STREAM_MAX_ROWS", "5000"))


OP = OpConfig()
KAFKA = KafkaConfig()
PRODUCER = ProducerConfig()