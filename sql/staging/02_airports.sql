-- Typed, trimmed airport reference data.
-- Grain: one row per airport in OurAirports.
DROP TABLE IF EXISTS staging.airports;
CREATE TABLE staging.airports AS
SELECT
    ident                                AS airport_ident,
    name                                 AS airport_name,
    type                                 AS airport_type,
    NULLIF(municipality, '')             AS municipality,
    iso_country                          AS country_code,
    latitude_deg::DOUBLE PRECISION       AS latitude,
    longitude_deg::DOUBLE PRECISION      AS longitude,
    NULLIF(UPPER(iata_code), '')         AS iata_code,
    NULLIF(UPPER(icao_code), '')         AS icao_code,
    NULLIF(UPPER(gps_code), '')          AS gps_code,
    NULLIF(UPPER(local_code), '')        AS local_code
FROM raw.airports;

ALTER TABLE staging.airports ADD PRIMARY KEY (airport_ident);
