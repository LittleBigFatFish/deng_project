-- Passenger names exactly as recorded. There is no reliable person id in the source,
-- so one name string = one passenger; we do NOT try to merge spellings or identify
-- unnamed people (ethics + data quality, see README).
-- Grain: one row per distinct name string; key 0 = name missing / illegible.
DROP TABLE IF EXISTS mart.dim_passenger;
CREATE TABLE mart.dim_passenger AS
SELECT
    ROW_NUMBER() OVER (ORDER BY passenger_name)::INTEGER AS passenger_key,
    passenger_name,
    BOOL_OR(is_known) AS is_known
FROM staging.flight_passengers
WHERE passenger_name IS NOT NULL
GROUP BY passenger_name;

INSERT INTO mart.dim_passenger (passenger_key, passenger_name, is_known)
VALUES (0, '(unknown)', FALSE);

ALTER TABLE mart.dim_passenger ADD PRIMARY KEY (passenger_key);
