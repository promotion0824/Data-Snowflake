-- ******************************************************************************************************************************
-- Create raw ontology tables
-- ******************************************************************************************************************************

CREATE TABLE IF NOT EXISTS raw.stage_tou_nyiso_load_forecast (
time_stamp    TIMESTAMP_NTZ,
capitl        INTEGER,
centrl        INTEGER,
dunwod        INTEGER,
genese        INTEGER,
hud_vl        INTEGER,
longil        INTEGER,
mhk_vl        INTEGER,
millwd        INTEGER,
nyc           INTEGER,
north         INTEGER,
west          INTEGER,
nyiso         INTEGER,
file_name 	  VARCHAR(1000),
_ingested_at  TIMESTAMP_NTZ DEFAULT SYSDATE()
);


CREATE TABLE IF NOT EXISTS transformed.tou_nyiso_load_forecast (
time_stamp    TIMESTAMP_NTZ,
capitl        INTEGER,
centrl        INTEGER,
dunwod        INTEGER,
genese        INTEGER,
hud_vl        INTEGER,
longil        INTEGER,
mhk_vl        INTEGER,
millwd        INTEGER,
nyc           INTEGER,
north         INTEGER,
west          INTEGER,
nyiso         INTEGER,
file_name 	  VARCHAR(1000),
_ingested_at  TIMESTAMP_NTZ DEFAULT SYSDATE(),
_last_updated_at  TIMESTAMP_NTZ DEFAULT SYSDATE()
);