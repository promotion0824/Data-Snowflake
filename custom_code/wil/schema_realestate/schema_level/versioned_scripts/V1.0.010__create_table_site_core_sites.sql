-- ------------------------------------------------------------------------------------------------------------------------------
-- Create table for all sites; need latitude and longitude for weather data
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE TRANSIENT TABLE IF NOT EXISTS raw.json_site_core_sites(
    _stage_record_id 	VARCHAR(36),
	 json_value 		VARIANT,
    _stage_file_name 	VARCHAR(1000),
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ(9),
    _staged_at 			TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS transformed.site_core_sites (
	id VARCHAR(36),
	customer_id VARCHAR(36),
	portfolio_id VARCHAR(36),
	name VARCHAR(255),
	code VARCHAR(36),
	address VARCHAR(255),
	state VARCHAR(20),
	postal_code VARCHAR(20),
	country VARCHAR(36),
	number_of_floors INTEGER,
	area VARCHAR(100),
	logo_id VARCHAR(36),
	latitude DECIMAL(9,6),
	longitude DECIMAL(9,6),
	time_zone_id  VARCHAR(100),
	status INTEGER,
	suburb VARCHAR(255),
	type INTEGER,
	construction_year INTEGER,
	site_code VARCHAR(36),
	source_created_date DATE,
	server VARCHAR(100),
	raw_json_value 		VARIANT,
	_is_active 			BOOLEAN DEFAULT true,
	_created_at 		TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_last_updated_at 	TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_stage_record_id 	STRING,
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ,
	_staged_at 			TIMESTAMP_NTZ
);

CREATE STREAM IF NOT EXISTS raw.json_site_core_sites_str 
    ON TABLE raw.json_site_core_sites
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE
;