-- Aircraft as recorded in the logs. The same tail number was used for different
-- aircraft over time (e.g. N908JE: Gulfstream and Boeing 727), so the natural key is
-- tail number + model, not tail number alone.
-- Grain: one row per (tail number, model).
DROP TABLE IF EXISTS mart.dim_aircraft;
CREATE TABLE mart.dim_aircraft AS
SELECT
    ROW_NUMBER() OVER (ORDER BY aircraft_tail, aircraft_model)::INTEGER AS aircraft_key,
    aircraft_tail,
    aircraft_model,
    MAX(aircraft_type)  AS aircraft_type,
    MAX(aircraft_seats) AS aircraft_seats,
    MIN(flight_date)    AS first_seen,
    MAX(flight_date)    AS last_seen
FROM staging.flight_passengers
GROUP BY aircraft_tail, aircraft_model;

ALTER TABLE mart.dim_aircraft ADD PRIMARY KEY (aircraft_key);
