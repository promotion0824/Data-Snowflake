-- ------------------------------------------------------------------------------------------------------------------------------
-- Create tables for Impact Scores
-- full Load in data loader
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE raw.json_impact_scores(
    _stage_record_id 	VARCHAR(36),
	 json_value 		VARIANT,
    _stage_file_name 	VARCHAR(1000),
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ(9),
    _staged_at 			TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE STREAM raw.json_impact_scores_str 
    ON TABLE raw.json_impact_scores
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;
CREATE OR REPLACE TABLE transformed.impact_scores(
	external_id 		VARCHAR(500),
	field_id 		    VARCHAR(500),
	id 					VARCHAR(36),
	insight_id 			VARCHAR(36),
	name 				VARCHAR(500),
    rule_id 			VARCHAR(500),
	unit 				VARCHAR(255),
	value 				FLOAT,
	_created_at 		TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_last_updated_at 	TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_stage_record_id 	STRING,
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ,
	_staged_at 			TIMESTAMP_NTZ
);