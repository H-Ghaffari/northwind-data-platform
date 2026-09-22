"""Topic creation.

Auto-creation is disabled on the broker, so topics are declared here. That
is the point: partition count and retention become reviewable in git, and a
mistyped topic name fails at startup instead of silently creating an empty
topic nobody ever reads from.
"""
from __future__ import annotations

import logging

from confluent_kafka.admin import AdminClient, NewTopic

from ..common.config import KAFKA

log = logging.getLogger(__name__)


def ensure_topics(topic_names: list[str]) -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA.bootstrap})

    existing = set(admin.list_topics(timeout=20).topics)
    missing = [t for t in topic_names if t not in existing]

    if not missing:
        log.info("all %d topics already exist", len(topic_names))
        return

    log.info("creating topics: %s", ", ".join(missing))

    futures = admin.create_topics([
        NewTopic(
            name,
            num_partitions=KAFKA.partitions,
            replication_factor=KAFKA.replication,
            config={
                # A week is long enough to replay after a consumer outage and
                # short enough that the broker's disk stays bounded. Nothing
                # depends on it for correctness: the source of truth is the
                # CDC log, and the replay position lives in ETL_Settings.
                "retention.ms": str(7 * 24 * 60 * 60 * 1000),
                "cleanup.policy": "delete",
            },
        )
        for name in missing
    ])

    for name, future in futures.items():
        try:
            future.result()
            log.info("created topic %s", name)
        except Exception as exc:  # noqa: BLE001
            if "already exists" in str(exc).lower():
                continue
            raise RuntimeError(f"could not create topic {name}: {exc}") from exc