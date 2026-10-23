-- Pre-aggregated answer to the main use-case question:
-- "which routes were flown how often, when, with which aircraft?"
-- Grain: one row per month x departure airport x arrival airport.
DROP TABLE IF EXISTS mart.agg_route_month;
CREATE TABLE mart.agg_route_month AS
SELECT
    d.year_month,
    f.dep_airport_key,
    f.arr_airport_key,
    COUNT(DISTINCT f.leg_key)       AS flight_legs,
    COUNT(*)                        AS passenger_entries,
    COUNT(DISTINCT f.aircraft_key)  AS aircraft_used,
    MAX(f.distance_km)              AS distance_km
FROM mart.fact_passenger_leg f
JOIN mart.dim_date d ON d.date_key = f.date_key
GROUP BY d.year_month, f.dep_airport_key, f.arr_airport_key;
