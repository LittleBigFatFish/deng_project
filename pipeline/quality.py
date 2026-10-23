"""Step 5: data-quality checks on the finished tables.

Each check is a SQL query that returns ONE number. The check passes if the number is
within the allowed range. Any failed check fails the Airflow task (red in the UI),
so problems are visible instead of silently ending up in an analysis.
"""
import logging

from pipeline.db import get_connection

log = logging.getLogger(__name__)

# (name, sql returning one number, min allowed, max allowed)
CHECKS = [
    ("raw.flights row count is plausible",
     "SELECT COUNT(*) FROM raw.flights", 4_500, 6_000),
    ("every raw flight record reached the fact table",
     "SELECT (SELECT COUNT(*) FROM raw.flights) - (SELECT COUNT(*) FROM mart.fact_passenger_leg)", 0, 0),
    ("no duplicate record ids in the fact table",
     "SELECT COUNT(*) - COUNT(DISTINCT record_id) FROM mart.fact_passenger_leg", 0, 0),
    ("no fact rows without a flight date",
     "SELECT COUNT(*) FROM mart.fact_passenger_leg WHERE date_key IS NULL", 0, 0),
    ("flight dates within the documented period 1990-2020",
     "SELECT COUNT(*) FROM mart.fact_passenger_leg WHERE date_key NOT BETWEEN '1990-01-01' AND '2020-12-31'", 0, 0),
    ("airport resolution rate >= 99 % of code references",
     """SELECT ROUND(100.0 * (COUNT(dep_airport_key) + COUNT(arr_airport_key)) / (2 * COUNT(*)), 2)
        FROM mart.fact_passenger_leg""", 99, 100),
    ("every resolved airport exists in dim_airport",
     """SELECT COUNT(*) FROM mart.fact_passenger_leg f
        WHERE (f.dep_airport_key IS NOT NULL AND f.dep_airport_key NOT IN (SELECT airport_key FROM mart.dim_airport))
           OR (f.arr_airport_key IS NOT NULL AND f.arr_airport_key NOT IN (SELECT airport_key FROM mart.dim_airport))""", 0, 0),
    ("every fact row has an aircraft",
     "SELECT COUNT(*) FROM mart.fact_passenger_leg WHERE aircraft_key IS NULL", 0, 0),
    ("route aggregate adds up to the fact table",
     "SELECT (SELECT SUM(passenger_entries) FROM mart.agg_route_month) - (SELECT COUNT(*) FROM mart.fact_passenger_leg)", 0, 0),
]


def run_checks() -> list[dict]:
    results, failed = [], []
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            for name, sql, lo, hi in CHECKS:
                cur.execute(sql)
                value = cur.fetchone()[0]
                ok = value is not None and lo <= value <= hi
                results.append({"check": name, "value": float(value) if value is not None else None, "ok": ok})
                log.info("%s %-55s value=%s (allowed %s..%s)", "PASS" if ok else "FAIL", name, value, lo, hi)
                if not ok:
                    failed.append(name)
    finally:
        conn.close()
    if failed:
        raise ValueError(f"{len(failed)} data-quality check(s) failed: {failed}")
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_checks()
