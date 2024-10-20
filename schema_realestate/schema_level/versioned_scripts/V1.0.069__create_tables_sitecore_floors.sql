-- ------------------------------------------------------------------------------------------------------------------------------
-- Create tables for directory core sql table

-- ------------------------------------------------------------------------------------------------------------------------------
CREATE TRANSIENT TABLE IF NOT EXISTS raw.json_site_core_floors(
    _stage_record_id 	VARCHAR(36),
	 json_value 		VARIANT,
    _stage_file_name 	VARCHAR(1000),
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ(9),
    _staged_at 			TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS transformed.site_core_floors(
	id					VARCHAR(36),
	site_id				VARCHAR(36),
	name				VARCHAR(255),
	floor_code			VARCHAR(36),
	sort_order			INTEGER,
    geometry			VARCHAR(16777216),
	is_decommissioned	BOOLEAN,
	model_reference		VARCHAR(36),
	is_site_wide		BOOLEAN,
	raw_json_value 		VARIANT,
	_is_active 			BOOLEAN DEFAULT true,
	_created_at 		TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_last_updated_at 	TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_stage_record_id 	STRING,
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ,
	_staged_at 			TIMESTAMP_NTZ
);