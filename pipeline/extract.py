"""Step 1: download a source file into the landing zone, one folder per run date.

landing/<source>/<run_date>/<filename>

Keeping each run's file (instead of overwriting one file) means we can always see
exactly which input a given run used, and a rerun for the same date replaces only
that date's file.
"""
import hashlib
import logging
from pathlib import Path

import requests

from pipeline.config import HTTP_TIMEOUT_S, LANDING_DIR, SOURCES

log = logging.getLogger(__name__)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download(source: str, run_date: str) -> dict:
    cfg = SOURCES[source]
    target_dir = LANDING_DIR / source / run_date
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / cfg["filename"]
    tmp = target.with_suffix(target.suffix + ".part")

    log.info("Downloading %s -> %s", cfg["url"], target)
    with requests.get(cfg["url"], stream=True, timeout=HTTP_TIMEOUT_S) as resp:
        resp.raise_for_status()  # HTTP errors fail the task -> Airflow retries it
        with open(tmp, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    tmp.replace(target)  # only a complete download ever gets the real file name

    result = {"source": source, "path": str(target), "sha256": sha256_of(target), "bytes": target.stat().st_size}
    log.info("Downloaded %s", result)
    return result


if __name__ == "__main__":  # manual use: python -m pipeline.extract flights 2026-10-23
    import sys

    logging.basicConfig(level=logging.INFO)
    print(download(sys.argv[1], sys.argv[2]))
