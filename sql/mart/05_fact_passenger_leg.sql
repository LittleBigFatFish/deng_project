-- Central fact table of the star schema.
-- Grain: one row = one passenger entry on one flight leg (one source record).
-- Count DISTINCT leg_key for "number of flights", COUNT(*) for "passenger entries".
DROP TABLE IF EXISTS mart.fact_passenger_leg;
CREATE TABLE mart.fact_passenger_leg AS
SELECT
    f.record_id,
    f.leg_key,
    f.flight_date                           AS date_key,
    ac.aircraft_key,
    f.dep_airport_ident                     AS dep_airport_key,
    f.arr_airport_ident                     AS arr_airport_key,
    COALESCE(p.passenger_key, 0)            AS passenger_key,
    f.flight_no,
    f.passenger_slot,
    f.data_source,
    -- great-circle distance; NULL when an airport could not be resolved
    ROUND((2 * 6371 * ASIN(SQRT(
        POWER(SIN(RADIANS(arr.latitude - dep.latitude) / 2), 2) +
        COS(RADIANS(dep.latitude)) * COS(RADIANS(arr.latitude)) *
        POWER(SIN(RADIANS(arr.longitude - dep.longitude) / 2), 2)
    )))::NUMERIC, 1)                        AS distance_km
FROM staging.flight_passengers f
JOIN mart.dim_aircraft ac
  ON ac.aircraft_tail = f.aircraft_tail AND ac.aircraft_model = f.aircraft_model
LEFT JOIN mart.dim_passenger p ON p.passenger_name = f.passenger_name
LEFT JOIN mart.dim_airport dep ON dep.airport_key = f.dep_airport_ident
LEFT JOIN mart.dim_airport arr ON arr.airport_key = f.arr_airport_ident;

ALTER TABLE mart.fact_passenger_leg ADD PRIMARY KEY (record_id);
CREATE INDEX ON mart.fact_passenger_leg (date_key);
CREATE INDEX ON mart.fact_passenger_leg (dep_airport_key, arr_airport_key);
