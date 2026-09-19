"""CDC producer.

Polls every active capture instance and publishes its changes to Kafka.

The order of operations in one pass is the whole correctness argument:

    read window  ->  produce  ->  flush and confirm  ->  advance watermark

A crash anywhere before the last step leaves the watermark where it was, so
the next pass reads the same window again. Republishing is absorbed by
ReplacingMergeTree in the warehouse; advancing first and failing after would
drop the window with nothing to show for it. This is the same reasoning the
batch path's CDC watermark already follows.
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import time

from confluent_kafka import KafkaException, Producer

from ..common import db
from ..common.config import KAFKA, OP, PRODUCER
from ..common.envelope import build_message
from ..common.sources import SOURCES
from .topics import ensure_topics

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("producer")

_running = True


def _stop(signum, _frame):
    """Finish the pass in flight, then exit.

    Killing mid-pass would be safe — the watermark has not moved — but it
    would mean replaying the window, and a clean stop costs one poll
    interval.
    """
    global _running
    log.info("received signal %s, finishing current pass", signum)
    _running = False


def _delivery_failures(producer: Producer) -> list[str]:
    """Block until every queued message is acknowledged or has failed."""
    failures: list[str] = []

    def report(err, msg):
        if err is not None:
            failures.append(f"{msg.topic()}: {err}")

    # The callback is attached per-message at produce time; this just drains.
    remaining = producer.flush(timeout=30)
    if remaining > 0:
        failures.append(f"{remaining} messages still unacknowledged after 30s")
    return failures


def process_source(producer: Producer, state: dict) -> int:
    """One pass over one capture instance. Returns rows published."""
    instance = state["capture_instance"]
    topic = state["topic_name"]

    if instance not in SOURCES:
        log.warning("%s is in Stream_State but not in the source registry", instance)
        return 0

    rows, window_end, warning = db.read_changes(
        instance, state["last_lsn"], PRODUCER.max_rows_per_pass
    )

    if warning:
        log.warning("%s: %s", instance, warning)
        db.record_error(instance, warning)

    if not rows:
        return 0

    # read_changes returns the end of the window as (lsn, commit time).
    window_lsn, window_time = window_end

    # Deletes on a dimension are captured but not carried forward, per the
    # brief. The rule lives in Stream_State rather than in code so a source's
    # behaviour is described by its row, and it is applied here rather than
    # in the consumer: a message nobody will act on should not occupy a
    # partition or a consumer's attention.
    propagate_deletes = bool(state["propagate_deletes"])

    published = 0
    failures: list[str] = []

    def on_delivery(err, msg):
        if err is not None:
            failures.append(f"{msg.topic()}[{msg.partition()}]: {err}")

    for row in rows:
        if row["__$operation"] == 1 and not propagate_deletes:
            continue

        key, value, _ = build_message(instance, row)

        try:
            producer.produce(topic, key=key.encode("utf-8"), value=value,
                             on_delivery=on_delivery)
        except BufferError:
            # The local queue is full, which means the broker is slower than
            # the reader. Drain and retry rather than dropping the message.
            producer.flush(timeout=30)
            producer.produce(topic, key=key.encode("utf-8"), value=value,
                             on_delivery=on_delivery)

        published += 1

    # Nothing survived the delete filter, so there is nothing to confirm —
    # but the window was read and must not be read again.
    if published == 0:
        db.advance_watermark(instance, window_lsn, 0, window_time)
        return 0

    failures.extend(_delivery_failures(producer))

    if failures:
        # The watermark stays where it is. Some messages may have landed and
        # will be republished next pass; that is the at-least-once bargain,
        # and it is the safe direction to be wrong in.
        message = f"{len(failures)} delivery failures: {failures[0]}"
        log.error("%s: %s", instance, message)
        db.record_error(instance, message)
        return 0


    db.advance_watermark(instance, window_lsn, published, window_time)
    log.info("%s -> %s | %d rows | window ends %s",
             instance, topic, published, window_lsn.hex())
    return published


def main() -> int:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    log.info("CDC producer starting")
    log.info("  source   %s:%s/%s", OP.host, OP.port, OP.database)
    log.info("  kafka    %s", KAFKA.bootstrap)
    log.info("  interval %.1fs", PRODUCER.poll_seconds)

    sources = db.load_active_sources()
    if not sources:
        log.error("Stream_State has no active sources. Run ./scripts/setup_stream.sh")
        return 1

    ensure_topics([s["topic_name"] for s in sources])

    producer = Producer({
        "bootstrap.servers": KAFKA.bootstrap,
        "client.id": "northwind-cdc-producer",

        # A retried send can otherwise be written twice when the broker's
        # acknowledgement is what got lost. Idempotence removes that case,
        # and implies acks=all — no message is confirmed until it is on disk.
        "enable.idempotence": True,

        # Small batching window. The point of this path is low latency, and
        # at Northwind's change rate a larger window would mostly add delay
        # to messages that could already have been sent.
        "linger.ms": 20,
        "compression.type": "lz4",
        "delivery.timeout.ms": 60000,
    })

    log.info("polling %d sources", len(sources))

    while _running:
        started = time.monotonic()
        total = 0

        try:
            # Re-read on every pass, so toggling is_active or repointing a
            # topic takes effect within one interval and needs no restart.
            for state in db.load_active_sources():
                try:
                    total += process_source(producer, state)
                except Exception as exc:  # noqa: BLE001
                    # One bad source must not stop the others. Its watermark
                    # has not moved, so its window is retried next pass.
                    log.exception("%s failed", state["capture_instance"])
                    db.record_error(state["capture_instance"], str(exc))

        except (pymssql_error := Exception) as exc:  # noqa: BLE001
            # Losing SQL Server entirely — the loop keeps running so the
            # producer reconnects on its own when the source comes back.
            log.error("pass failed: %s", exc)

        if total:
            log.info("pass published %d rows in %.2fs",
                     total, time.monotonic() - started)

        # Sleep in slices so a shutdown signal is noticed promptly rather
        # than after a full interval.
        elapsed = time.monotonic() - started
        remaining = max(0.0, PRODUCER.poll_seconds - elapsed)
        while remaining > 0 and _running:
            time.sleep(min(0.5, remaining))
            remaining -= 0.5

    log.info("flushing before exit")
    producer.flush(timeout=30)
    log.info("producer stopped")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KafkaException as exc:
        log.critical("kafka error: %s", exc)
        sys.exit(1)