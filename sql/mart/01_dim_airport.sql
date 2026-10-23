-- Airports that appear in the flight logs, with readable name, country and coordinates.
-- Grain: one row per airport (key: airport_key = OurAirports ident).
DROP TABLE IF EXISTS mart.dim_airport;
CREATE TABLE mart.dim_airport AS
SELECT
    a.airport_ident AS airport_key,
    a.airport_name,
    a.airport_type,
    a.municipality,
    a.country_code,
    a.latitude,
    a.longitude,
    a.iata_code,
    a.icao_code
FROM staging.airports a
WHERE a.airport_ident IN (
    SELECT dep_airport_ident FROM staging.flight_passengers
    UNION
    SELECT arr_airport_ident FROM staging.flight_passengers
);

ALTER TABLE mart.dim_airport ADD PRIMARY KEY (airport_key);
