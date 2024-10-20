-- ------------------------------------------------------------------------------------------------------------------------------
-- Create raw account details table
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE raw.json_widgets (
  _stage_record_id 	VARCHAR(36),
  json_value 		    VARIANT,
  _stage_file_name 	VARCHAR(1000),
  _loader_run_id 		VARCHAR(36),
  _ingested_at 		  TIMESTAMP_NTZ(9),
  _staged_at 			  TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE TABLE  transformed.widgets (
    site_id	          VARCHAR(36),
    portfolio_id       VARCHAR(36),
    customer_id       VARCHAR(36),
    widget_id	        VARCHAR(36),
    position          INTEGER,
    type              INTEGER,
    metadata	        VARIANT,
    _created_at       TIMESTAMP_LTZ,
   _last_updated_at   TIMESTAMP_LTZ,
   _ingested_at       TIMESTAMP_LTZ,
   _staged_at 			  TIMESTAMP_NTZ(9),
   _loader_run_id     VARCHAR(36)
);
