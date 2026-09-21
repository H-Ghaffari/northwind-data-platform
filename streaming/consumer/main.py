"""Consumer loop: read from Kafka, apply to NorthwindRT, commit.

Offsets are committed after the warehouse write, never before. A crash in
between replays the message, and ReplacingMergeTree absorbs the repeat —
the same at-least-once bargain the producer's watermark makes.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import signal
import sys

from confluent_kafka import Consumer, KafkaError
from scd_rules import DIMENSIONS, validate

from ..common.config import KAFKA
from .warehouse import KEYS, client
from . import facts, handlers, telemetry, audit

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("consumer")

TOPICS = [
    # Reference and dimension sources first, though Kafka gives no
    # cross-topic ordering — a fact arriving before its dimension creates an
    # inferred member, which is designed behaviour rather than a race to be
    # avoided.
    "nw.cdc.categories", "nw.cdc.region",
    "nw.cdc.suppliers", "nw.cdc.shippers", "nw.cdc.territories",
    "nw.cdc.customers", "nw.cdc.products", "nw.cdc.employees",
    "nw.cdc.orders", "nw.cdc.order_details", "nw.cdc.employee_territories",
]

SURROGATE_KEYS = {
    "DimCustomer": "customer_key", "DimProducts": "product_key",
    "DimEmployees": "employee_key", "DimSuppliers": "supplier_key",
    "DimShippers": "shipper_key", "DimTerritories": "territory_key",
}

_running = True


def _stop(signum, _frame):
    global _running
    log.info("received signal %s, finishing current message", signum)
    _running = False


def main() -> int:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    ch = client()

    # The contract asserts things about a schema this process cannot see.
    # Checked before the first message rather than discovered on the first
    # failed insert — or worse, on a column nothing ever writes to.
    problems = validate(ch, os.environ.get("DW_RT_DB", "NorthwindRT"))
    if problems:
        for problem in problems:
            log.error("contract: %s", problem)
        return 1

    for table, key_column in SURROGATE_KEYS.items():
        log.info("  %-16s keys continue from %s",
                 table, KEYS.prime(ch, table, key_column) + 1)

    consumer = Consumer({
        "bootstrap.servers": KAFKA.bootstrap,
        "group.id": "northwind-rt-consumer",
        # A topic added later replays from the beginning, which is correct:
        # the snapshot already holds that state and the repeat is absorbed.
        "auto.offset.reset": "earliest",
        # Committed by hand, after the write. Auto-commit would acknowledge
        # messages the warehouse had not yet accepted.
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
    })
    consumer.subscribe(TOPICS)
    log.info("consuming %d topics", len(TOPICS))

    while _running:
        msg = consumer.poll(1.0)

        if msg is None:
            telemetry.flush(ch)
            audit.flush()
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("kafka: %s", msg.error())
            continue

        applied_at = dt.datetime.now()
        envelope = json.loads(msg.value())
        target = envelope["target_table"]

        try:
            # Dispatch on capture instance, not target table: Orders and
            # Order Details both write to FactOrders but do opposite things
            # — one fans a header out to its lines, the other writes one line.
            instance = envelope["capture_instance"]

            if target in DIMENSIONS:
                outcome, rows = handlers.handle_dimension(ch, envelope, applied_at)
            elif target in handlers.REFERENCE:
                outcome, rows = handlers.handle_reference(ch, envelope, applied_at)
            elif instance == "dbo_Orders":
                outcome, rows = facts.handle_order(ch, envelope, applied_at)
            elif instance == "dbo_OrderDetails":
                outcome, rows = facts.handle_order_detail(ch, envelope, applied_at)
            elif instance == "dbo_EmployeeTerritories":
                outcome, rows = facts.handle_employee_territory(ch, envelope, applied_at)
            else:
                outcome, rows = "skipped", 0

            telemetry.event(envelope, applied_at, outcome, rows, msg)
            audit.record(envelope, applied_at, outcome, rows, msg)

            if outcome not in ("unchanged", "skipped"):
                log.info("%s %s %s -> %s rows",
                         target, envelope["business_key"], outcome, rows)

            consumer.commit(msg, asynchronous=False)

        except Exception as exc:  # noqa: BLE001
            # The offset stays put, so the message is retried on the next
            # poll. A permanently bad message would loop — visible in
            # StreamEvents as an error count that climbs, which is the point
            # of recording it there rather than only in the container log.
            log.exception("%s %s failed", target, envelope.get("business_key"))
            telemetry.event(envelope, applied_at, "error", 0, msg, str(exc)[:500])
            audit.record(envelope, applied_at, "error", 0, msg, str(exc)[:500])
            telemetry.flush(ch, force=True)
            audit.flush(force=True)

        telemetry.flush(ch)
        audit.flush()

    telemetry.flush(ch, force=True)
    audit.flush(force=True)
    consumer.close()
    ch.close()
    log.info("consumer stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())