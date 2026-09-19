"""Turning a CDC change row into a Kafka message.

The message is a self-describing envelope rather than a bare row, so a
consumer can route and interpret it without holding a copy of the producer's
configuration. Anything read from the envelope is something the consumer
then does not have to be told separately.
"""
from __future__ import annotations

import datetime as dt
import decimal
import json
from typing import Any

from .sources import SOURCES, build_key

# CDC's own bookkeeping columns. Carried as metadata where useful, but kept
# out of the data payload — the consumer works with source columns only.
_CDC_COLUMNS = {
    "__$start_lsn", "__$end_lsn", "__$seqval",
    "__$operation", "__$update_mask", "__$command_id", "__$commit_time",
}

_OPERATION = {1: "delete", 2: "insert", 4: "update"}


def _encode(value: Any) -> Any:
    """JSON has no type for most of what a relational row contains."""
    if isinstance(value, decimal.Decimal):
        # str, not float. Northwind prices are money values, and a float
        # round-trip can change the last cent of a revenue total.
        return str(value)
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, dt.time):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return value


def build_message(capture_instance: str, row: dict) -> tuple[str, bytes, dict]:
    """Return (key, serialised value, metadata) for one change row."""
    source = SOURCES[capture_instance]
    operation = _OPERATION.get(row["__$operation"], "unknown")

    commit_time: dt.datetime = row["__$commit_time"]
    data = {k: _encode(v) for k, v in row.items() if k not in _CDC_COLUMNS}

    envelope = {
        "capture_instance": capture_instance,
        "source_table": source.source_table,
        "target_table": source.target_table,
        "operation": operation,
        "business_key": build_key(capture_instance, row),

        # When SQL Server committed the transaction. The consumer subtracts
        # this from its own clock to get the lag that the near-real-time
        # claim is measured by, so it has to travel with the message.
        "source_time": commit_time.isoformat(),

        # Position in the CDC log. Not used for routing; it is what makes a
        # message traceable back to the exact source transaction when a row
        # in the warehouse looks wrong.
        "lsn": row["__$start_lsn"].hex(),
        "seqval": row["__$seqval"].hex(),

        "data": data,
    }

    key = envelope["business_key"]
    value = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
    return key, value, envelope