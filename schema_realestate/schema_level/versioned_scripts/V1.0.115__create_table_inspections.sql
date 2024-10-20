-- ------------------------------------------------------------------------------------------------------------------------------
-- Create tables for directory core sql table

-- ------------------------------------------------------------------------------------------------------------------------------
CREATE TRANSIENT TABLE IF NOT EXISTS raw.json_inspections(
    _stage_record_id 	VARCHAR(36),
	 json_value 		VARIANT,
    _stage_file_name 	VARCHAR(1000),
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ(9),
    _staged_at 			TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS transformed.inspections(
    id                         VARCHAR(36),
    site_id                    VARCHAR(36),
    name                       VARCHAR(200),
    floor_code                 VARCHAR(10),
    zone_id                    VARCHAR(36),
    asset_id                   VARCHAR(36),
    assigned_workgroup_id      VARCHAR(36),
    frequency_in_hours         NUMERIC(12,0),
    start_date                 TIMESTAMP_NTZ,
    end_date                   TIMESTAMP_NTZ,
    last_record_id             VARCHAR(36),
    is_archived                BOOLEAN,
    sort_order                 NUMERIC(12,0),
    frequency                  NUMERIC(12,0),
    frequency_unit             VARCHAR(20),
    twin_id                    VARCHAR(250),
    raw_json_value 				VARIANT,
	_created_at 				TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_last_updated_at 			TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_stage_record_id 			STRING,
	_loader_run_id 				VARCHAR(36),
	_ingested_at 				TIMESTAMP_NTZ,
	_staged_at 					TIMESTAMP_NTZ
);