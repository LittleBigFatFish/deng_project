"""Central configuration: where the sources live and what we expect from them."""
import os
from pathlib import Path

# Where downloaded files are kept (a Docker volume inside the Airflow container).
LANDING_DIR = Path(os.environ.get("DENG_LANDING_DIR", "./landing"))

# SQL files for the transformation layers (mounted into the Airflow container).
SQL_DIR = Path(os.environ.get("DENG_SQL_DIR", Path(__file__).resolve().parent.parent / "sql"))

HTTP_TIMEOUT_S = 60

SOURCES = {
    "flights": {
        "url": "https://raw.githubusercontent.com/valinux/flight-logs-explorer/main/flights.json",
        "filename": "flights.json",
        "format": "json_table",  # {"headers": [...], "rows": [[...], ...]}
        "raw_table": "raw.flights",
        "required_columns": [
            "ID", "Date", "Aircraft Model", "Aircraft Tail #", "Aircraft Type", "# of Seats",
            "DEP: Code", "ARR: Code", "DEP", "ARR", "Flight_No.", "Pass #",
            "First Name", "Last Name", "First Last", "Known", "Data Source",
        ],
        # Historic, closed dataset: ~5,000 rows. A big deviation means a broken download.
        "min_rows": 4_500,
        "max_rows": 6_000,
    },
    "airports": {
        "url": "https://davidmegginson.github.io/ourairports-data/airports.csv",
        "filename": "airports.csv",
        "format": "csv",
        "raw_table": "raw.airports",
        "required_columns": [
            "ident", "type", "name", "latitude_deg", "longitude_deg", "iso_country",
            "municipality", "icao_code", "iata_code", "gps_code", "local_code",
        ],
        # Reference data that grows slowly: ~85,000 airports worldwide.
        "min_rows": 70_000,
        "max_rows": 150_000,
    },
}
