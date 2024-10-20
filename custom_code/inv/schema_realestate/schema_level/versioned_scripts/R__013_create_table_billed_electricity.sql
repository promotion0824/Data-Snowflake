-- ******************************************************************************************************************************
-- Create table
-- ******************************************************************************************************************************

CREATE OR REPLACE TABLE transformed.billed_electricity (
	date_local DATE,
	timestamp_local TIMESTAMP_NTZ(9),
	telemetry_value FLOAT,
	capability_id VARCHAR(255),
	trend_id VARCHAR(100),
	model_id VARCHAR(255),
	unit VARCHAR(100),
	description VARCHAR(1000),
	asset_id VARCHAR(255),
	model_id_asset VARCHAR(255),
	site_id VARCHAR(100),
	building_id VARCHAR(100),
	building_name VARCHAR(1000),
	_last_updated_at TIMESTAMP_NTZ(9)
);