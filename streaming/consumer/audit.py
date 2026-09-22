"""MongoDB audit log: one document per event the consumer applies.

StreamEvents in ClickHouse keeps numbers — lag, counts, outcomes — for
drawing a line on a dashboard. This keeps the whole event, for the other
question: what exactly happened to one record, and why. A document store
fits that; a columnar one does not, since full payloads would bloat every
part and slow the aggregates StreamEvents exists for.

Each document carries the LSN, the Kafka coordinates and the SCD outcome,
so one change can be traced from the source transaction through the broker
to the warehouse row it produced.

Writes are batched, and a failure is logged and dropped rather than
raised. A pipeline that stops because its audit store is unreachable has
made observability worse, not better — the same choice ETL_Log makes on
the batch side and StreamEvents makes here.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import time
import json

from pymongo import ASCENDING, MongoClient
from pymongo.errors import PyMongoError

log = logging.getLogger("consumer.audit")

FLUSH_DOCS = 50
FLUSH_SECONDS = 2.0

# Matches StreamEvents: long enough to investigate an incident, short enough
# that the audit store never becomes a storage problem of its own.
RETENTION_SECONDS = 30 * 24 * 3600

_collection = None
_buffer: list[dict] = []
_last_flush = time.monotonic()
_last_warning = 0.0


def _warn(message: str, *args) -> None:
    """At most one warning a minute, so an outage does not flood the log."""
    global _last_warning
    if time.monotonic() - _last_warning > 60:
        log.warning(message, *args)
        _last_warning = time.monotonic()


def _events():
    """The collection, connected lazily and reconnected after a failure.

    Lazy rather than at startup so a MongoDB that comes up after the consumer
    is picked up on the next flush instead of disabling audit for the life of
    the process.
    """
    global _collection
    if _collection is not None:
        return _collection

    try:
        client = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=3000)
        collection = client[os.environ.get("MONGO_DB", "northwind_audit")]["events"]

        # Idempotent: create_index is a no-op when the index already exists.
        collection.create_index(
            [("applied_at", ASCENDING)], expireAfterSeconds=RETENTION_SECONDS)
        collection.create_index(
            [("capture_instance", ASCENDING), ("business_key", ASCENDING)])

        _collection = collection
        log.info("audit log connected")
    except (PyMongoError, KeyError) as exc:
        _warn("audit log unavailable, events will not be recorded: %s", exc)
    return _collection


def record(envelope: dict, applied_at: dt.datetime, outcome: str,
           rows: int, msg, error: str = "") -> None:
    source_time = dt.datetime.fromisoformat(envelope["source_time"])
    doc = {
        "applied_at": applied_at,
        "source_time": source_time,
        "lag_ms": int((applied_at - source_time).total_seconds() * 1000),
        "capture_instance": envelope["capture_instance"],
        "source_table": envelope.get("source_table", ""),
        "target_table": envelope["target_table"],
        "operation": envelope["operation"],
        "outcome": outcome,
        "business_key": envelope["business_key"],
        "rows_written": rows,
        "kafka": {"topic": msg.topic(), "partition": msg.partition(),
                  "offset": msg.offset()},
        "lsn": envelope.get("lsn", ""),
        "data": envelope.get("data", {}),
        "error": error,
    }

    # The same document again, as one JSON string. It exists for Logstash.
    #
    # The MongoDB input flattens documents and guesses types as it goes: any
    # string that looks like a number becomes one. An LSN loses its leading
    # zeros and stops identifying a transaction, a postcode of "05021"
    # becomes 5021, and a business key is a number for an order but a string
    # for an order line — which Elasticsearch then rejects as a type
    # conflict. An index template cannot restore digits that were dropped
    # before the document reached it.
    #
    # JSON carries its types with it, so the pipeline parses this field and
    # discards the plugin's guesses. The structured fields above stay, since
    # they are what makes the collection queryable from mongosh.
    doc["payload"] = json.dumps({
        **{k: v for k, v in doc.items() if k not in ("applied_at", "source_time")},
        "applied_at": applied_at.isoformat(timespec="milliseconds"),
        "source_time": source_time.isoformat(timespec="milliseconds"),
    }, ensure_ascii=False, default=str)

    _buffer.append(doc)


def flush(force: bool = False) -> None:
    global _last_flush, _collection

    due = force or len(_buffer) >= FLUSH_DOCS or \
        (time.monotonic() - _last_flush) >= FLUSH_SECONDS
    if not _buffer or not due:
        return

    events = _events()
    if events is None:
        # Dropped rather than held: an unbounded buffer during a long outage
        # would turn an audit problem into a memory problem.
        _buffer.clear()
        _last_flush = time.monotonic()
        return

    try:
        events.insert_many(_buffer, ordered=False)
    except PyMongoError as exc:
        _warn("audit write failed, %d events dropped: %s", len(_buffer), exc)
        _collection = None   # reconnect on the next flush
    _buffer.clear()
    _last_flush = time.monotonic()