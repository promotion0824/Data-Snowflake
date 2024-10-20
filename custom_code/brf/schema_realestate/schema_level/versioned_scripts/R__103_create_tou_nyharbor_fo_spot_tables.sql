-- ******************************************************************************************************************************
-- Create raw ontology tables
-- ******************************************************************************************************************************

CREATE TABLE IF NOT EXISTS raw.stage_tou_nyharbor_nyiso_fo_spot (
date	      DATE,
fo_spot_price DECIMAL(12,3),
file_name 	  VARCHAR(1000),
_ingested_at  TIMESTAMP_NTZ DEFAULT SYSDATE()
);

CREATE TABLE IF NOT EXISTS transformed.tou_nyharbor_nyiso_fo_spot (
date	      DATE,
fo_spot_price DECIMAL(12,3),
file_name 	  VARCHAR(1000),
_ingested_at  TIMESTAMP_NTZ DEFAULT SYSDATE(),
_last_updated_at  TIMESTAMP_NTZ DEFAULT SYSDATE()
);