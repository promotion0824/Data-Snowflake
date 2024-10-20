-- ------------------------------------------------------------------------------------------------------------------------------
-- Create raw account details table
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE raw.json_connectors(
  _stage_record_id 	VARCHAR(36),
  json_value 		    VARIANT,
  _stage_file_name 	VARCHAR(1000),
  _loader_run_id 		VARCHAR(36),
  _ingested_at 		  TIMESTAMP_NTZ(9),
  _staged_at 			  TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE TABLE  transformed.connectors (
  id			                VARCHAR(36),
  client_id			          VARCHAR(36),
  name                    VARCHAR(255),
  connector_type          VARCHAR(255),
  site_id                 VARCHAR(36),
  is_enabled		          BOOLEAN,
  is_archived			        BOOLEAN,
  source_last_updated_at  TIMESTAMP_LTZ,
  _last_updated_at        TIMESTAMP_LTZ,
  _ingested_at            TIMESTAMP_LTZ,
  _loader_run_id          VARCHAR(36)
);
