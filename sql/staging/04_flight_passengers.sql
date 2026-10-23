-- Cleaned flight-log records: typed columns, normalised codes, resolved airports.
-- Grain: one row per passenger entry in the source logs (= one raw row, key: record_id).
--
-- Cleaning applied (why: see README "Transformations"):
--   * airport codes trimmed + upper-cased ("TEb" -> "TEB"), then resolved via the
--     lookup/alias tables to a reference airport
--   * placeholder names ("?", "? ?", "") turned into NULL
--   * known flag "Yes"/"No" -> boolean
--   * flight number "" -> NULL, kept as text (FOIA records use ids like "FOIA-137 1")
--   * a flight leg key built from date + aircraft + route + flight number, because the
--     source "Unique ID" is not unique
DROP TABLE IF EXISTS staging.flight_passengers;
CREATE TABLE staging.flight_passengers AS
WITH typed AS (
    SELECT
        id::INTEGER                                   AS record_id,
        date::DATE                                    AS flight_date,
        UPPER(TRIM(aircraft_tail_num))                AS aircraft_tail,
        TRIM(aircraft_model)                          AS aircraft_model,
        TRIM(aircraft_type)                           AS aircraft_type,
        NULLIF(num_of_seats, '')::INTEGER             AS aircraft_seats,
        UPPER(TRIM(dep_code))                         AS dep_code,
        UPPER(TRIM(arr_code))                         AS arr_code,
        NULLIF(TRIM(dep), '')                         AS dep_label,
        NULLIF(TRIM(arr), '')                         AS arr_label,
        NULLIF(TRIM(flight_no), '')                   AS flight_no,
        NULLIF(TRIM(pass_num), '')                    AS passenger_slot,
        NULLIF(NULLIF(TRIM(first_last), ''), '? ?')   AS passenger_name,
        (known = 'Yes')                               AS is_known,
        NULLIF(TRIM(comment), '')                     AS comment,
        data_source
    FROM raw.flights
)
SELECT
    t.*,
    dep.airport_ident                                 AS dep_airport_ident,
    arr.airport_ident                                 AS arr_airport_ident,
    MD5(CONCAT_WS('|', t.flight_date, t.aircraft_tail, t.dep_code, t.arr_code, t.flight_no)) AS leg_key
FROM typed t
LEFT JOIN staging.airport_code_lookup dep ON dep.code = t.dep_code
LEFT JOIN staging.airport_code_lookup arr ON arr.code = t.arr_code;

ALTER TABLE staging.flight_passengers ADD PRIMARY KEY (record_id);
