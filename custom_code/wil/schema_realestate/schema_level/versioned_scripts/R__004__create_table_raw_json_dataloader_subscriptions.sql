-- ------------------------------------------------------------------------------------------------------------------------------
-- Create raw account details table
-- this is in schema_realestate instead of monitoring because it needs to be deployed for each env db.
-- ------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE raw.json_monitoring_dataloader_subscriptions(
  _stage_record_id 	VARCHAR(36),
  json_value 		    VARIANT,
  _stage_file_name 	VARCHAR(1000),
  _loader_run_id 		VARCHAR(36),
  _ingested_at 		  TIMESTAMP_NTZ(9),
  _staged_at 			  TIMESTAMP_NTZ(9)
);