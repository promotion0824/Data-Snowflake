-- ------------------------------------------------------------------------------------------------------------------------------
-- Create tables for Insights
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE raw.json_impact_scores(
    _stage_record_id 	VARCHAR(36),
	 json_value 		VARIANT,
    _stage_file_name 	VARCHAR(1000),
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ(9),
    _staged_at 			TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE TABLE transformed.impact_scores(
	id 					VARCHAR(36),
	insight_id 			VARCHAR(36),
	name 				VARCHAR(500),
	value 				FLOAT,
	unit 				VARCHAR(255),
	customer_id 		VARCHAR(36),
	_created_at 		TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_last_updated_at 	TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_stage_record_id 	STRING,
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ,
	_staged_at 			TIMESTAMP_NTZ
);