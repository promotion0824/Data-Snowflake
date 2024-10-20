-- ------------------------------------------------------------------------------------------------------------------------------
-- Create tables for Insights
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE raw.json_insight_occurrences(
    _stage_record_id 	VARCHAR(36),
	 json_value 		VARIANT,
    _stage_file_name 	VARCHAR(1000),
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ(9),
    _staged_at 			TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE STREAM raw.json_insight_occurrences_str 
    ON TABLE raw.json_insight_occurrences
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE TABLE transformed.insight_occurrences(
    id	            VARCHAR(36),
    insight_id	    VARCHAR(36),
    occurrence_id 	VARCHAR(36),
    is_faulted      BOOLEAN,
    is_valid        BOOLEAN,
    started         TIMESTAMP_NTZ,
    ended           TIMESTAMP_NTZ,
    text 	        VARCHAR(8000),
	raw_json_value 		VARIANT,
	_created_at 		TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_last_updated_at 	TIMESTAMP_NTZ DEFAULT SYSDATE(),
	_stage_record_id 	STRING,
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ,
	_staged_at 			TIMESTAMP_NTZ
);