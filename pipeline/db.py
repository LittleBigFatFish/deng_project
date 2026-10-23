"""PostgreSQL connection helper. Credentials come from environment variables, never from code."""
import os

import psycopg2


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("DENG_DB_HOST", "localhost"),
        port=int(os.environ.get("DENG_DB_PORT", "5432")),
        dbname=os.environ["DENG_DB_NAME"],
        user=os.environ["DENG_DB_USER"],
        password=os.environ["DENG_DB_PASSWORD"],
    )
