-- ------------------------------------------------------------------------------------------------------------------------------
-- Create tables for digital commissioning validation
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.json_twins_validation_results(
  _stage_record_id 	VARCHAR(36),
  json_value 		VARIANT,
  _stage_file_name 	VARCHAR(1000),
  _loader_run_id 	VARCHAR(36),
  _ingested_at 		TIMESTAMP_NTZ(9),
  _staged_at 		 TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS raw.json_twins_validation_aggregate_scores(
  _stage_record_id 	VARCHAR(36),
  json_value 		VARIANT,
  _stage_file_name 	VARCHAR(1000),
  _loader_run_id 	VARCHAR(36),
  _ingested_at 		TIMESTAMP_NTZ(9),
  _staged_at 		 TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS raw.json_twins_static_validation_scores(
  _stage_record_id 	VARCHAR(36),
  json_value 		VARIANT,
  _stage_file_name 	VARCHAR(1000),
  _loader_run_id 	VARCHAR(36),
  _ingested_at 		TIMESTAMP_NTZ(9),
  _staged_at 		 TIMESTAMP_NTZ(9)
);

CREATE TABLE IF NOT EXISTS raw.json_twins_validation_connectivity_scores(
  _stage_record_id 	VARCHAR(36),
  json_value 		VARIANT,
  _stage_file_name 	VARCHAR(1000),
  _loader_run_id 	VARCHAR(36),
  _ingested_at 		TIMESTAMP_NTZ(9),
  _staged_at 		 TIMESTAMP_NTZ(9)
);
