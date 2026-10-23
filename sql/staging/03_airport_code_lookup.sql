-- Resolve every code that can appear in the logs (IATA, ICAO, FAA local code, ...)
-- to exactly one airport. The logs mix IATA ("TEB") and ICAO ("TIST") codes, so we
-- index each airport under all of its codes and pick the best match per code.
-- Grain: one row per code.
DROP TABLE IF EXISTS staging.airport_code_lookup;
CREATE TABLE staging.airport_code_lookup AS
WITH candidates AS (
    SELECT iata_code  AS code, airport_ident, airport_type, 1 AS priority FROM staging.airports WHERE iata_code  IS NOT NULL
    UNION ALL
    SELECT icao_code,          airport_ident, airport_type, 2 FROM staging.airports WHERE icao_code  IS NOT NULL
    UNION ALL
    SELECT airport_ident,      airport_ident, airport_type, 3 FROM staging.airports
    UNION ALL
    SELECT gps_code,           airport_ident, airport_type, 4 FROM staging.airports WHERE gps_code   IS NOT NULL
    UNION ALL
    -- US FAA codes such as "X21" or "27K" (small airfields without IATA code)
    SELECT local_code,         airport_ident, airport_type, 5 FROM staging.airports WHERE local_code IS NOT NULL AND country_code = 'US'
)
SELECT DISTINCT ON (code)
    code,
    airport_ident,
    'reference'::TEXT AS match_type
FROM candidates
ORDER BY code, priority, (airport_type = 'closed'), airport_ident;

-- Manual aliases win over the automatic lookup.
DELETE FROM staging.airport_code_lookup WHERE code IN (SELECT code_in_logs FROM staging.airport_code_alias);
INSERT INTO staging.airport_code_lookup (code, airport_ident, match_type)
SELECT code_in_logs, airport_ident, 'alias' FROM staging.airport_code_alias;

ALTER TABLE staging.airport_code_lookup ADD PRIMARY KEY (code);
