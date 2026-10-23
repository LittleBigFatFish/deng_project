"""Step 4: build a warehouse layer by running its SQL files in name order.

All files of one layer run in ONE transaction: either the whole layer is rebuilt,
or nothing changes. Every SQL file drops and recreates its table, so reruns are safe.
"""
import logging

from pipeline.config import SQL_DIR
from pipeline.db import get_connection

log = logging.getLogger(__name__)


def run_layer(layer: str) -> list[str]:
    files = sorted((SQL_DIR / layer).glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"no SQL files in {SQL_DIR / layer}")
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            for f in files:
                log.info("Running %s", f.name)
                cur.execute(f.read_text(encoding="utf-8"))
    finally:
        conn.close()
    return [f.name for f in files]


if __name__ == "__main__":  # manual use: python -m pipeline.transform staging
    import sys

    logging.basicConfig(level=logging.INFO)
    print(run_layer(sys.argv[1]))
