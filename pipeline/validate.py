"""Step 2: check a downloaded file BEFORE it is allowed into the database.

A broken or truncated download must fail here, so it never replaces good data.
"""
import csv
import json
import logging
from pathlib import Path

from pipeline.config import SOURCES

log = logging.getLogger(__name__)


class ValidationError(Exception):
    pass


def read_header_and_count(source: str, path: Path) -> tuple[list[str], int]:
    fmt = SOURCES[source]["format"]
    if fmt == "json_table":
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return list(data["headers"]), len(data["rows"])
    if fmt == "csv":
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            return header, sum(1 for _ in reader)
    raise ValueError(f"unknown format {fmt}")


def validate(source: str, path: str) -> dict:
    cfg = SOURCES[source]
    try:
        header, rows = read_header_and_count(source, Path(path))
    except (json.JSONDecodeError, KeyError, StopIteration, UnicodeDecodeError) as e:
        raise ValidationError(f"{source}: file cannot be parsed: {e}") from e

    missing = [c for c in cfg["required_columns"] if c not in header]
    if missing:
        raise ValidationError(f"{source}: missing columns {missing}")
    if not cfg["min_rows"] <= rows <= cfg["max_rows"]:
        raise ValidationError(f"{source}: {rows} rows, expected {cfg['min_rows']}-{cfg['max_rows']}")

    log.info("%s OK: %d rows, %d columns", source, rows, len(header))
    return {"source": source, "rows": rows, "columns": len(header)}
