-- ------------------------------------------------------------------------------------------------------------------------------
-- Create tables for Insights
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE raw.json_insights(
    _stage_record_id 	VARCHAR(36),
	 json_value 		VARIANT,
    _stage_file_name 	VARCHAR(1000),
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ(9),
    _staged_at 			TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE STREAM raw.json_insights_str 
    ON TABLE raw.json_insights
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE TABLE transformed.insights(
	id					VARCHAR(36),
	site_id				VARCHAR(36),
	sequence_number		VARCHAR(100),
	equipment_id		VARCHAR(36),
	external_id 		VARCHAR(500),
	external_metadata	VARIANT,
	type				INTEGER,
	name				VARCHAR(1000),
	description			VARCHAR(8000),
	priority			INTEGER,
	status				INTEGER,
	external_status     VARCHAR(36),
	created_date		TIMESTAMP_NTZ,
	updated_date		TIMESTAMP_NTZ,
	last_occurred_date	TIMESTAMP_NTZ,
	detected_date		TIMESTAMP_NTZ,
	source_type			INTEGER,
	source_id			VARCHAR(36),
    rule_id			    VARCHAR(100),
    rule_name			VARCHAR(1000),
    twin_id             VARCHAR(100),
	twin_name           VARCHAR(255),
    primary_model_id	VARCHAR(255),
	points_json			VARIANT,
    recommendation      VARCHAR(16777216),
	reported			BOOLEAN,
	state				INTEGER,
	new_occurrence		BOOLEAN,
	occurrence_count	INTEGER,
	created_user_id		VARCHAR(100),
	customer_id			VARCHAR(36),
	raw_json_value 		VARIANT,
	_created_at 		TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_last_updated_at 	TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_stage_record_id 	STRING,
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ,
	_staged_at 			TIMESTAMP_NTZ
);