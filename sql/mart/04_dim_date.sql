-- Calendar for the period covered by the logs.
-- Grain: one row per day.
DROP TABLE IF EXISTS mart.dim_date;
CREATE TABLE mart.dim_date AS
SELECT
    d::DATE                         AS date_key,
    EXTRACT(YEAR    FROM d)::INT    AS year,
    EXTRACT(QUARTER FROM d)::INT    AS quarter,
    EXTRACT(MONTH   FROM d)::INT    AS month,
    TO_CHAR(d, 'YYYY-MM')           AS year_month,
    EXTRACT(ISODOW  FROM d)::INT    AS iso_day_of_week
FROM GENERATE_SERIES(
    (SELECT DATE_TRUNC('year', MIN(flight_date)) FROM staging.flight_passengers),
    (SELECT MAX(flight_date) FROM staging.flight_passengers),
    INTERVAL '1 day'
) AS d;

ALTER TABLE mart.dim_date ADD PRIMARY KEY (date_key);
