-- ******************************************************************************************************************************
-- Create initial objects to support dataloader monitoring
-- ******************************************************************************************************************************

CREATE TABLE IF NOT EXISTS raw.json_dataloader_copyactivity(
  _stage_record_id 	VARCHAR(36),
  json_value 		    VARIANT,
  _stage_file_name 	VARCHAR(1000),
  _loader_run_id 		VARCHAR(36),
  _ingested_at 		  TIMESTAMP_NTZ(9),
  _staged_at 			  TIMESTAMP_NTZ(9)
);

CREATE STREAM IF NOT EXISTS raw.json_dataloader_copyactivity_str
    ON TABLE raw.json_dataloader_copyactivity
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE
;

CREATE TABLE IF NOT EXISTS transformed.dataloader_copyactivity(
	unique_name VARCHAR(255),
	data_source_name VARCHAR(255),
	data_source_type VARCHAR(255),
	region VARCHAR(255),
	trigger_name VARCHAR(255),
	loader_type VARCHAR(255),
	status  VARCHAR(255),
	start_time TIMESTAMP_NTZ, 
	duration_seconds INTEGER,
	bytes_read INTEGER,
	bytes_written INTEGER,
	rows_read INTEGER,
	rows_copied INTEGER,
	rows_skipped INTEGER,
	files_read INTEGER,
	files_written INTEGER,
	files_skipped INTEGER,
	throughput_kbps FLOAT, 
	last_watermark TIMESTAMP_NTZ,
	source_query VARCHAR(16777216),
	destination_path VARCHAR(16777216),
	full_output VARCHAR(16777216), 
	source_entity_lastupdated TIMESTAMP_NTZ,
	data_source_id INTEGER,
	source_entity_id INTEGER,
	loader_run_id VARCHAR(36),
	pipeline_run_id VARCHAR(36),
	_created_at		      	TIMESTAMP_NTZ,
	_last_updated_at		    TIMESTAMP_NTZ,
	_stage_record_id 	      	STRING,
	_loader_run_id 		    VARCHAR(36),
	_ingested_at 				TIMESTAMP_NTZ,
	_staged_at				TIMESTAMP_NTZ
)
;