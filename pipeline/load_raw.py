"""Step 3: load a validated file into the raw schema (full load, all columns as TEXT).

Why full load and not incremental:
  * flights.json is a closed, historic dataset - there are no "new rows" to pick up.
  * airports.csv is a small reference file without a reliable "changed since" field.
  Reloading everything is cheap (seconds) and always gives the same result.

Why this is safe to rerun:
  * DROP/CREATE + COPY + log entry run in ONE transaction (DDL is transactional in
    Postgres). If anything fails, everything is rolled back and the previous good
    data stays untouched; readers never see a half-loaded table.
  * If the file's SHA-256 equals the last successful load, the reload is skipped.

Raw tables keep the source values as text on purpose: typing and cleaning happen in
the staging layer, so a bad value in the source can never make the raw load fail.
"""
import csv
import io
import json
import logging
import re

from pipeline.config import SOURCES
from pipeline.db import get_connection

log = logging.getLogger(__name__)

LOAD_LOG_DDL = """
CREATE TABLE IF NOT EXISTS raw.load_log (
    load_id     BIGSERIAL PRIMARY KEY,
    source      TEXT        NOT NULL,
    run_id      TEXT        NOT NULL,
    file_path   TEXT        NOT NULL,
    sha256      TEXT        NOT NULL,
    row_count   INTEGER,
    status      TEXT        NOT NULL,  -- loaded | skipped_unchanged
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def to_column_name(header: str) -> str:
    """'Aircraft Tail #' -> 'aircraft_tail_num', 'DEP: Code' -> 'dep_code'."""
    name = header.strip().lower().replace("#", "num")
    return re.sub(r"[^a-z0-9]+", "_", name).strip("_")


def file_as_csv(source: str, path: str) -> tuple[list[str], io.StringIO, int]:
    """Return (header, CSV buffer without header, row count) for COPY."""
    fmt = SOURCES[source]["format"]
    buf = io.StringIO()
    writer = csv.writer(buf)
    if fmt == "json_table":
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        header = data["headers"]
        for row in data["rows"]:
            writer.writerow(["" if v is None else v for v in row])
        count = len(data["rows"])
    else:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            count = 0
            for row in reader:
                writer.writerow(row)
                count += 1
    buf.seek(0)
    return header, buf, count


def load(source: str, path: str, sha256: str, run_id: str) -> dict:
    table = SOURCES[source]["raw_table"]
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            return _load(cur, source, table, path, sha256, run_id)
    finally:
        conn.close()


def _load(cur, source: str, table: str, path: str, sha256: str, run_id: str) -> dict:
    cur.execute(LOAD_LOG_DDL)

    cur.execute(
        "SELECT sha256 FROM raw.load_log WHERE source = %s AND status = 'loaded' "
        "ORDER BY loaded_at DESC LIMIT 1",
        (source,),
    )
    last = cur.fetchone()
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (table,))
    table_exists = cur.fetchone()[0]
    if last and last[0] == sha256 and table_exists:
        cur.execute(
            "INSERT INTO raw.load_log (source, run_id, file_path, sha256, status) "
            "VALUES (%s, %s, %s, %s, 'skipped_unchanged')",
            (source, run_id, path, sha256),
        )
        log.info("%s unchanged (sha256 %s) - skipping reload", source, sha256[:12])
        return {"source": source, "status": "skipped_unchanged"}

    header, buf, count = file_as_csv(source, path)
    columns = [to_column_name(h) for h in header]

    # Recreate the table from the file's header so the raw layer always mirrors the source.
    cur.execute(f"DROP TABLE IF EXISTS {table}")
    cols_ddl = ", ".join(f'"{c}" TEXT' for c in columns)
    cur.execute(f"CREATE TABLE {table} ({cols_ddl})")
    col_list = ", ".join(f'"{c}"' for c in columns)
    cur.copy_expert(f"COPY {table} ({col_list}) FROM STDIN WITH (FORMAT csv)", buf)
    cur.execute(
        f"ALTER TABLE {table} ADD COLUMN _run_id TEXT DEFAULT %s, "
        f"ADD COLUMN _loaded_at TIMESTAMPTZ DEFAULT now()",
        (run_id,),
    )

    cur.execute(
        "INSERT INTO raw.load_log (source, run_id, file_path, sha256, row_count, status) "
        "VALUES (%s, %s, %s, %s, %s, 'loaded')",
        (source, run_id, path, sha256, count),
    )
    # the caller's `with conn` block commits; an exception rolls everything back
    log.info("Loaded %d rows into %s", count, table)
    return {"source": source, "status": "loaded", "rows": count}
