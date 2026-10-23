"""Airflow DAG: flight records batch pipeline.

    extract_flights  -> validate_flights  -> load_raw_flights  --\
                                                                  +-> build_staging -> build_mart -> quality_checks
    extract_airports -> validate_airports -> load_raw_airports --/

Scheduling: weekly. flights.json is historic and never changes; airports.csv is
reference data that changes occasionally (renamed codes), so a weekly refresh is enough.

Recovery:
  * retries:   every task retries 3x with exponential back-off (e.g. GitHub timeout)
  * reruns:    every task is idempotent (full reload in one transaction, tables rebuilt
               from scratch), so "Clear" on any task in the UI is safe
  * backfills: `airflow dags backfill -s <start> -e <end> flights_pipeline` runs past
               intervals; each run stores its download under landing/<source>/<run date>/
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline import extract, load_raw, quality, transform, validate
from pipeline.config import SOURCES

default_args = {
    "owner": "deng-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="flights_pipeline",
    schedule="@weekly",
    start_date=datetime(2026, 9, 1),
    catchup=False,          # no automatic catch-up; past runs only via explicit backfill
    max_active_runs=1,      # runs rebuild the same tables, so never run two at once
    default_args=default_args,
    tags=["deng", "flights"],
    doc_md=__doc__,
)
def flights_pipeline():
    loaded = []
    for source in SOURCES:

        @task(task_id=f"extract_{source}")
        def extract_task(src: str, ds=None) -> dict:
            return extract.download(src, ds)

        @task(task_id=f"validate_{source}")
        def validate_task(file_info: dict) -> dict:
            validate.validate(file_info["source"], file_info["path"])
            return file_info

        @task(task_id=f"load_raw_{source}")
        def load_task(file_info: dict, run_id=None) -> dict:
            return load_raw.load(file_info["source"], file_info["path"], file_info["sha256"], run_id)

        loaded.append(load_task(validate_task(extract_task(source))))

    @task
    def build_staging():
        return transform.run_layer("staging")

    @task
    def build_mart():
        return transform.run_layer("mart")

    @task(retries=0)  # a failed quality check is a data problem, retrying won't fix it
    def quality_checks():
        return quality.run_checks()

    loaded >> build_staging() >> build_mart() >> quality_checks()


flights_pipeline()
