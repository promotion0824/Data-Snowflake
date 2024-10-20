-- ******************************************************************************************************************************
-- Create raw ontology tables
-- ******************************************************************************************************************************

CREATE TABLE IF NOT EXISTS raw.stage_tou_hhng_spot (
date	      DATE,
ng_spot_price DECIMAL(12,2),
file_name 	  VARCHAR(1000),
_ingested_at  TIMESTAMP_NTZ DEFAULT SYSDATE()
);


CREATE TABLE IF NOT EXISTS transformed.tou_hhng_spot (
date	      DATE,
ng_spot_price DECIMAL(12,2),
file_name 	  VARCHAR(1000),
_ingested_at  TIMESTAMP_NTZ DEFAULT SYSDATE(),
_last_updated_at  TIMESTAMP_NTZ DEFAULT SYSDATE()
);