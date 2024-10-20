-- ------------------------------------------------------------------------------------------------------------------------------
-- Create tables for telemetry
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.stage_telemetry (
	connector_id VARCHAR(36),
	dt_id VARCHAR(16777216),
	external_id VARCHAR(16777216),
	trend_id VARCHAR(36),
	captured_at TIMESTAMP_NTZ(9),
	enqueued_at TIMESTAMP_NTZ(9),
	scalar_value VARIANT,
	latitude FLOAT,
	longitude FLOAT,
	altitude FLOAT,
	properties VARIANT,
	exported_time TIMESTAMP_NTZ(9), 
	_ingested_at TIMESTAMP_NTZ(9),
	stage_file_name VARCHAR(2000)
);

CREATE TABLE IF NOT EXISTS transformed.telemetry (
	date_local DATE,
	timestamp_local TIMESTAMP_NTZ(9),
	timestamp_utc TIMESTAMP_NTZ(9),
	site_id VARCHAR(36),
	trend_id VARCHAR(36),
	external_id VARCHAR(16777216),
	telemetry_value FLOAT,
	connector_id VARCHAR(36),
	dt_id VARCHAR(16777216),
	enqueued_at TIMESTAMP_NTZ(9),
	latitude FLOAT,
	longitude FLOAT,
	altitude FLOAT,
	properties VARIANT,
	exported_time TIMESTAMP_NTZ(9),
	_created_at TIMESTAMP_NTZ(9),
	_last_updated_at TIMESTAMP_NTZ(9),
	stage_file_name VARCHAR(2000)
)
;
