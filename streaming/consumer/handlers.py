"""What to do with an event, by target table."""
from __future__ import annotations

import datetime as dt
import logging

from scd_rules import DIMENSIONS

from . import lookups, scd
from .warehouse import VERSIONS, coerce, current_row, insert_rows, stored_columns

log = logging.getLogger("consumer.handlers")

REFERENCE = {
    "RefCategories": ("CategoryID", "CategoryName", "category_id", "category_name",
                      "DimProducts", "category_name"),
    "RefRegion": ("RegionID", "RegionDescription", "region_id", "region_description",
                  "DimTerritories", "region_description"),
}


def handle_dimension(ch, envelope: dict, applied_at: dt.datetime) -> tuple[str, int]:
    dim = DIMENSIONS[envelope["target_table"]]
    data = envelope["data"]

    incoming = scd.build_row(ch, dim, data)

    columns = [name for name, _ in stored_columns(ch, dim.table)]
    current = current_row(
        ch, dim.table, dim.alternate_key, incoming[dim.alternate_key], columns
    )

    outcome = scd.classify(dim, incoming, current)
    rows = scd.rows_to_write(
        dim, outcome, incoming, current, applied_at, stored=set(columns)
    )

    # An inferred member is matched like any other existing row and takes the
    # type 1 path, which is how a stub gets filled in. Clearing the cache
    # entry keeps a later lookup from returning the key of a row that has
    # since been superseded.
    lookups.clear_surrogate(dim.table, incoming[dim.alternate_key])

    return outcome, insert_rows(ch, dim.table, rows)


def handle_reference(ch, envelope: dict, applied_at: dt.datetime) -> tuple[str, int]:
    """Apply a change to a reference table, then to what quoted it.

    The batch path rebuilds these joins from scratch every night, so a
    renamed category simply appears in the next run. The streaming path has
    to propagate it: the dimensions hold the name, not the id, and nothing
    else will ever revisit them.

    The old name is read before the reference row is replaced — after, there
    would be no way to find the rows that carried it.
    """
    table = envelope["target_table"]
    src_id, src_name, id_col, name_col, dim_table, dim_col = REFERENCE[table]
    data = envelope["data"]

    types = dict(stored_columns(ch, table))
    ref_id = coerce(data.get(src_id), types[id_col])
    new_name = coerce((data.get(src_name) or "").strip(), types[name_col])

    existing = current_row(ch, table, id_col, ref_id, [id_col, name_col])
    old_name = existing[name_col] if existing else None

    insert_rows(ch, table, [{
        id_col: ref_id, name_col: new_name, "_version": VERSIONS.take(),
    }])
    lookups.clear_reference(table)

    if old_name is None or old_name == new_name:
        return "new" if old_name is None else "unchanged", 1

    # The dimension column is type 2 in both cases, so each affected row gets
    # a version rather than an overwrite: a product really did belong to the
    # old category name while that was its name.
    dim = DIMENSIONS[dim_table]
    dim_stored = {name for name, _ in stored_columns(ch, dim_table)}
    written = 1
    for stale in lookups.rows_referencing(ch, dim_table, dim_col, old_name):
        updated = dict(stale)
        updated[dim_col] = new_name
        written += insert_rows(ch, dim_table, scd.rows_to_write(
            dim, "type2",
            {c.target: updated[c.target] for c in dim.columns}
            | {dim.alternate_key: stale[dim.alternate_key]},
            stale, applied_at, stored=dim_stored,
        ))

    log.info("%s %s -> %s cascaded to %s rows in %s",
             table, old_name, new_name, written - 1, dim_table)
    return "type2", written