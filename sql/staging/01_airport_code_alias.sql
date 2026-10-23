-- Manual alias map for airport codes in the flight logs that do not match the
-- current OurAirports reference data. Found during profiling (see README).
-- Grain: one row per code as written in the logs.
DROP TABLE IF EXISTS staging.airport_code_alias;
CREATE TABLE staging.airport_code_alias (
    code_in_logs  TEXT PRIMARY KEY,
    airport_ident TEXT NOT NULL,   -- OurAirports "ident"
    reason        TEXT NOT NULL
);

INSERT INTO staging.airport_code_alias (code_in_logs, airport_ident, reason) VALUES
    ('PBI',      'KPBI', 'Palm Beach Intl. was re-coded PBI -> DJT in the reference data'),
    ('PBI-OFF',  'KPBI', 'PBI with a free-text suffix'),
    ('TIST-LSJ', 'TIST', 'St. Thomas (TIST) with a free-text suffix'),
    ('FAJS',     'FAOR', 'Old ICAO code of Johannesburg O.R. Tambo'),
    ('WRRR',     'WADD', 'Old ICAO code of Denpasar / Bali'),
    ('TPQF',     'TQPF', 'Typo of Anguilla TQPF (letters swapped)'),
    ('GVAL',     'LSGG', 'Not a valid code; log text says Geneva, Switzerland'),
    ('MPPV',     'TBPB', 'Not a valid code; log text says Seawell (old name of Barbados airport)');
