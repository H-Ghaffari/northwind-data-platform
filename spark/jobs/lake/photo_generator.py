"""Populate the data lake with employee photographs.

Northwind ships with an empty Photo column, which the brief points out
directly. Rather than skip the unstructured half of the project, a
deterministic avatar is generated per employee: initials on a colour
derived from the employee code.

Deterministic matters here. Regenerating produces byte-identical files, so
the checksums in the catalogue stay valid and a rerun is a no-op rather
than a churn of "new" files.

SVG is written directly rather than through an imaging library. The output
is a real file in the lake either way, and avoiding Pillow keeps the
container image smaller for something that is not the point of the
exercise.
"""

from __future__ import annotations

import hashlib
import os
import sys
from datetime import datetime
from pathlib import Path

from common.spark_utils import dw_client, log

LAKE_ROOT = Path(os.environ.get("LAKE_ROOT", "/data/lake"))
PHOTO_DIR = LAKE_ROOT / "employees"

CANVAS = 256

# Distinct, readable at small sizes, and stable per index.
PALETTE = [
    "#2E5A88", "#8B3A3A", "#3D6B4A", "#7A5C2E",
    "#5B4A7A", "#2E7A7A", "#8B5A2E", "#4A4A6B",
    "#6B2E5A",
]


def _initials(full_name: str) -> str:
    parts = [p for p in full_name.split() if p]
    if not parts:
        return "?"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][0] + parts[-1][0]).upper()


def _svg(initials: str, colour: str) -> str:
    """A square avatar: initials centred on a solid background."""
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{CANVAS}" height="{CANVAS}" viewBox="0 0 {CANVAS} {CANVAS}">'
        f'<rect width="{CANVAS}" height="{CANVAS}" fill="{colour}"/>'
        f'<text x="50%" y="50%" dy="0.35em" text-anchor="middle" '
        f'font-family="Helvetica, Arial, sans-serif" font-size="104" '
        f'font-weight="600" fill="#FFFFFF">{initials}</text>'
        f'</svg>'
    )


def _read_employees() -> list[tuple[int, str]]:
    """Employee codes and names from the warehouse."""
    client = dw_client()
    try:
        rows = client.query(
            "SELECT employee_alternate_key, full_name "
            "FROM v_DimEmployees_Current "
            "ORDER BY employee_alternate_key"
        ).result_rows
    finally:
        client.close()

    if not rows:
        raise RuntimeError(
            "DimEmployees is empty. Load the dimensions before generating "
            "lake files — the lake catalogue keys on employee_alternate_key."
        )
    return [(int(code), str(name)) for code, name in rows]


def generate_photos() -> list[dict]:
    """Write one avatar per employee and describe each file."""
    PHOTO_DIR.mkdir(parents=True, exist_ok=True)

    employees = _read_employees()
    log.info("Generating %s avatars into %s", len(employees), PHOTO_DIR)

    catalogue: list[dict] = []
    for code, full_name in employees:
        colour = PALETTE[code % len(PALETTE)]
        content = _svg(_initials(full_name), colour)

        file_name = f"{code}.svg"
        file_path = PHOTO_DIR / file_name
        file_path.write_text(content, encoding="utf-8")

        raw = content.encode("utf-8")
        catalogue.append({
            "employee_code": code,
            "file_name": file_name,
            # The path as the lake exposes it, not as this process happens to
            # see it. A consumer on another mount should still resolve it.
            "file_path": f"/data/lake/employees/{file_name}",
            "content_type": "image/svg+xml",
            "size_bytes": len(raw),
            "checksum_sha256": hashlib.sha256(raw).hexdigest(),
            "width_px": CANVAS,
            "height_px": CANVAS,
            "ingested_at": datetime.utcnow().replace(microsecond=0),
            "source": "generated",
        })

    log.info("Wrote %s files", len(catalogue))
    return catalogue


def register_catalogue(catalogue: list[dict]) -> int:
    """Write the file descriptions into the lake catalogue table.

    ReplacingMergeTree keyed on employee_code, so re-registering replaces
    rather than duplicates — which is what makes this safe to rerun after
    regenerating the files.
    """
    if not catalogue:
        log.warning("Nothing to register")
        return 0

    columns = [
        "employee_code", "file_name", "file_path", "content_type",
        "size_bytes", "checksum_sha256", "width_px", "height_px",
        "ingested_at", "source",
    ]
    rows = [tuple(entry[c] for c in columns) for entry in catalogue]

    client = dw_client()
    try:
        client.insert("LakeEmployeePhotos", rows, column_names=columns)
        log.info("Registered %s file(s) in LakeEmployeePhotos", len(rows))
        return len(rows)
    finally:
        client.close()


def verify() -> None:
    """Check that every catalogued file is present and unmodified.

    A catalogue that points at files nobody can open is worse than no
    catalogue: it looks complete. The checksum catches a file that was
    replaced rather than merely missing.
    """
    client = dw_client()
    try:
        rows = client.query(
            "SELECT employee_code, file_path, size_bytes, checksum_sha256 "
            "FROM LakeEmployeePhotos FINAL ORDER BY employee_code"
        ).result_rows
    finally:
        client.close()

    missing, corrupt = [], []
    for code, path, size, checksum in rows:
        local = Path(path)
        if not local.exists():
            missing.append(code)
            continue
        raw = local.read_bytes()
        if len(raw) != size or hashlib.sha256(raw).hexdigest() != checksum:
            corrupt.append(code)

    log.info(
        "Verified %s catalogued file(s): %s missing, %s modified",
        len(rows), len(missing), len(corrupt),
    )
    if missing:
        log.error("Missing files for employee codes: %s", missing)
    if corrupt:
        log.error("Modified files for employee codes: %s", corrupt)


def run() -> int:
    catalogue = generate_photos()
    written = register_catalogue(catalogue)
    verify()
    return written


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
