#!/bin/bash
# Runs once, on the first start of an empty Postgres volume.
# - creates a separate database + role for Airflow's own metadata
# - creates the three layers of our data warehouse as schemas
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE ROLE airflow LOGIN PASSWORD '${AIRFLOW_DB_PASSWORD}';
  CREATE DATABASE airflow OWNER airflow;

  CREATE SCHEMA IF NOT EXISTS raw;
  CREATE SCHEMA IF NOT EXISTS staging;
  CREATE SCHEMA IF NOT EXISTS mart;
EOSQL
