-- ------------------------------------------------------------------------------------------------------------------------------
-- Create table
-- full Load in data loader
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.json_ontology_models(
    _stage_record_id 	VARCHAR(36),
	 json_value 		VARIANT,
    _stage_file_name 	VARCHAR(1000),
	_loader_run_id 		VARCHAR(36),
	_ingested_at 		TIMESTAMP_NTZ(9),
    _staged_at 			TIMESTAMP_NTZ(9)
);

-- We can use show initial rows because we are using a full replace in the merge.
CREATE OR REPLACE STREAM raw.json_ontology_models_str
    ON TABLE raw.json_ontology_models
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

CREATE TABLE IF NOT EXISTS transformed.ontology_models (
	id                VARCHAR(16777216),
	is_decommissioned VARCHAR(10),
	export_time       TIMESTAMP_NTZ(9),
	display_name      VARCHAR(16777216),
	model_definition  VARIANT,
	deleted           VARCHAR(16777216),
	all_extends       VARCHAR(16777216)
);