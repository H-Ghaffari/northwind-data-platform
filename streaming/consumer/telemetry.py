"""StreamEvents: what the near-real-time claim is measured by."""
from __future__ import annotations

import datetime as dt
import time

from .warehouse import insert_rows

_buffer: list[dict] = []
_last_flush = time.monotonic()

# Every insert into a MergeTree creates a part the background merge has to
# clean up. One per event would make the telemetry table more expensive than
# the pipeline it measures, so rows are batched. Two seconds is short enough
# that a dashboard on a five-second refresh never shows an empty panel.
FLUSH_ROWS = 50
FLUSH_SECONDS = 2.0


def record(**fields) -> None:
    _buffer.append(fields)


def flush(ch, force: bool = False) -> None:
    global _last_flush
    due = force or len(_buffer) >= FLUSH_ROWS or (time.monotonic() - _last_flush) >= FLUSH_SECONDS
    if not _buffer or not due:
        return
    try:
        insert_rows(ch, "StreamEvents", _buffer)
    except Exception:
        # Telemetry that can stop the pipeline has made observability worse,
        # not better — the same choice ETL_Log makes on the batch side.
        pass
    _buffer.clear()
    _last_flush = time.monotonic()


def event(envelope, applied_at: dt.datetime, outcome: str,
          rows: int, msg, error: str = "") -> None:
    source_time = dt.datetime.fromisoformat(envelope["source_time"])
    record(
        event_time=applied_at,
        source_time=source_time,
        lag_ms=int((applied_at - source_time).total_seconds() * 1000),
        capture_instance=envelope["capture_instance"],
        topic=msg.topic(),
        target_table=envelope["target_table"],
        operation=envelope["operation"],
        scd_outcome=outcome,
        business_key=envelope["business_key"],
        rows_written=rows,
        kafka_partition=msg.partition(),
        kafka_offset=msg.offset(),
        error=error,
    )