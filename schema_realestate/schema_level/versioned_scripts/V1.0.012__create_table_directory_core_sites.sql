-- ------------------------------------------------------------------------------------------------------------------------------
-- Create tables for directory core sql table

-- ------------------------------------------------------------------------------------------------------------------------------
CREATE TRANSIENT TABLE IF NOT EXISTS raw.json_directory_core_sites(
    _stage_record_id 	VARCHAR(36),
	 json_value 		VARIANT,
    _stage_file_name 	VARCHAR(1000),
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ(9),
    _staged_at 			TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS transformed.directory_core_sites (
	site_id 					VARCHAR(36) NOT NULL,
	portfolio_id 				VARCHAR(36) NOT NULL,
	customer_id 				VARCHAR(36) NOT NULL,
	name 						VARCHAR(100) NULL,
	time_zone 					VARCHAR(100) NOT NULL,
	raw_json_value 				VARIANT NOT NULL,
	_is_active 					BOOLEAN NOT NULL DEFAULT true,
	_created_at 				TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_last_updated_at 			TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_stage_record_id 			STRING NOT NULL,
	_loader_run_id 				VARCHAR(36) NOT NULL,
	_ingested_at 				TIMESTAMP_NTZ NOT NULL,
	_staged_at 					TIMESTAMP_NTZ NOT NULL
);
